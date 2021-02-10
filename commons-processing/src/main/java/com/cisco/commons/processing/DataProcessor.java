package com.cisco.commons.processing;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import com.cisco.commons.processing.retry.FailureHandler;
import com.cisco.commons.processing.retry.ResultHandler;
import com.cisco.commons.processing.retry.RetryExecutor;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Data processor.
 * Processing data objects by multiple parallel consumers with ability to override pending objects
 * tasks for saving redundant tasks. <br/>
 * Highlight features:
 * <ul>
 * <li> Ability to override pending/running objects tasks for saving redundant tasks.
 * <li> Asynchronous retry mechanism for data objects processing tasks.
 * <li> Save potential memory by holding data objects instead of tasks class instances.
 * <li> No redundant live threads where there are no pending tasks.
 * </ul>
 * This is useful for example case where multiple notification received on same data object IDs in a 
 * time window where the previous data objects are still pending processing since the
 * internal thread pool is running other tasks up to the core pool size limit. The data processing
 * logic involves fetching the object from the DB and parsing the result. In this case, the
 * new notifications will override the same data objects entries, and each data object will be
 * fetched and processed with hopefully a single task instead of multiple times. <br/>
 * <br/>
 * DataProcessor.aggregate() vs threadPool.execute() - by the above example: <br/>
 * threadPool.execute:
 * <ul>
 * <li> 10 notifications arrive on data object with key 'x'.
 * <li> 10 similar tasks are created and executed via the thread pool for fetching and processing the
 * same object, 9 of them are redundant.
 * </ul>
 * DataProcessor.aggregate():
 * <ul>
 * <li> 10 notifications arrive on data object with key 'x'.
 * <li> 10 notifications are mapped to the same single queue map entry.
 * <li> 1 task is created and executed via the thread pool.
 * </ul>
 * <br/>
 * Theoretically, this solution can fit also for persistent messaging processing by replacing
 * the data map implementation with persistent map using one of the persistent key-value 
 * storage products. <br/>
 * <br/>
 * 
 * @author Liran Mendelovich
 * 
 * Copyright 2021 Cisco Systems
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
@Slf4j
public class DataProcessor {
	
	private Integer numOfThreads;
	private Long retryDelay;
	private TimeUnit retryDelayTimeUnit;
	private int retries;
	private RetryExecutor retryExecutor;
	private ExecutorService pool;
	private AtomicInteger runningTasksCount;
	private AtomicInteger submittedOrRunningTasksCount;
	private DataObjectProcessor dataObjectProcessor;
	private Map<Object, DataObject> dataMap = new LinkedHashMap<>();
	private Map<Object, DataObject> runningDataMap = new ConcurrentHashMap<>();
	private ResultHandler resultHandler;
	private DataObjectProcessResultHandler dataObjectProcessResultHandler;
	private FailureHandler failureHandler;
	private boolean shouldAggregateIfAlreadyRunning = false;

	@Builder
	public DataProcessor(Integer numOfThreads, Long retryDelay, TimeUnit retryDelayTimeUnit, int retries,
			DataObjectProcessor dataObjectProcessor, DataObjectProcessResultHandler dataObjectProcessResultHandler,
			FailureHandler failureHandler, boolean shouldAggregateIfAlreadyRunning) {
		super();
		this.numOfThreads = numOfThreads;
		this.retryDelay = retryDelay;
		this.retryDelayTimeUnit = retryDelayTimeUnit;
		this.retries = retries;
		this.dataObjectProcessor = dataObjectProcessor;
		this.dataObjectProcessResultHandler = dataObjectProcessResultHandler;
		this.resultHandler = buildResultHandler();
		this.failureHandler = failureHandler;
		this.shouldAggregateIfAlreadyRunning = shouldAggregateIfAlreadyRunning;
		if (dataObjectProcessor == null) {
			throw new IllegalArgumentException("dataObjectProcessor is missing");
		}
		if (numOfThreads < 1) {
			throw new IllegalArgumentException("numOfThreads is less than 1");
		}
		this.pool = Executors.newFixedThreadPool(numOfThreads);
		retryExecutor = RetryExecutor.builder().build();
		runningTasksCount = new AtomicInteger(0);
		submittedOrRunningTasksCount = new AtomicInteger(0);
	}

	private ResultHandler buildResultHandler() {
		return (Supplier<Boolean> supplier, Boolean result) -> {
			try {
				submittedOrRunningTasksCount.decrementAndGet();
				if (supplier instanceof DataProcessorSupplier) {
					DataObject dataObject = ((DataProcessorSupplier)supplier).getDataObject();
					runningDataMap.remove(dataObject.getKey());
					dataObjectProcessResultHandler.handleResult(dataObject, result);
				} else {
					log.error("Unexpected supplier");
				}
				processDataObjects();
			} catch (Exception e) {
				log.error("Error handleResult." + e.getMessage(), e);
			}
		};
	}
	
	/**
	 * Aggregate data object to be executed when available.
	 * @param key - key for the data.
	 * @param data - the data.
	 * @throws Exception in case of error.
	 */
	public void aggregate(Object key, Object data) throws ProcessingException {
		try {
			DataObject dataObject = DataObject.builder().key(key).data(data).build();
			aggregate(dataObject);
		} catch (Exception e) {
			throw new ProcessingException("Error aggregating: " + e.getMessage(), e);
		}
	}
	
	protected void aggregate(DataObject dataObject) throws Exception {
		if (!shouldAggregateIfAlreadyRunning && runningDataMap.containsKey(dataObject.getKey())) {
			log.info("Not aggregating {} as it is already have a running task.");
			return;
		}
		addDataObject(dataObject);
		processDataObjects();
	}

	protected void addDataObject(DataObject dataObject) throws Exception {
		synchronized (dataMap) {
			dataMap.put(dataObject.getKey(), dataObject);
		}
	}
	
	public void processDataObjects() throws Exception  {
		log.debug("Processing data objects. tasksCount: {}", runningTasksCount);
		int count = 0;
		int times = 2;
		while (submittedOrRunningTasksCount.get() >= numOfThreads && count < times) {
			if (runningTasksCount.get() >= numOfThreads) {
		    	log.debug("Tasks count is larger or equals number of threads. Not processing.");
		    	return;
		    }
			count++;
			if (count < times) {
				Thread.sleep(5);
			}
		}
		processNextDataObject();
    }
	
	protected void processNextDataObject() throws Exception {
		log.debug("processNextDataObject");
		DataObject dataObject = poll();
        if (dataObject != null) {
        	runningDataMap.put(dataObject.getKey(), dataObject);
        	processDataObject(dataObject);
        	postProcess(dataObject);
        }
    }
	
	protected void postProcess(DataObject dataObject) throws Exception {
		
	}

	protected DataObject poll() throws Exception {
			synchronized (dataMap) {
	    	Iterator<Entry<Object, DataObject>> it = dataMap.entrySet().iterator();
	    	if (it.hasNext()) {
	    		Entry<Object, DataObject> entry = it.next();
	    		it.remove();
	    		return entry.getValue();
	    	}
	    	return null;
		}
    }
	
	private void processDataObject(DataObject dataObject) {
		Supplier<Boolean> supplier = new DataProcessorSupplier(dataObject);
		submittedOrRunningTasksCount.incrementAndGet();
		retryExecutor.executeAsync(supplier, pool, retryDelay, retryDelayTimeUnit, retries, 
				resultHandler, failureHandler);
	}
	
	@AllArgsConstructor
	private class DataProcessorSupplier implements Supplier<Boolean> {
		
		@Getter
		private DataObject dataObject;

		@Override
		public Boolean get() {
			try {
				runningTasksCount.incrementAndGet();
				return dataObjectProcessor.process(dataObject);
			} catch (Exception e) {
				log.error("Error processing data object by dataObjectProcessor: " + e.getMessage(), e);
				return false;
			} finally {
				runningTasksCount.decrementAndGet();
			}
		}
	}

	public void shutdown() {
		try {
			pool.shutdownNow();
		} catch (Exception e) {
			log.error("Error shutdown." + e.getMessage());
		}
	}
}
