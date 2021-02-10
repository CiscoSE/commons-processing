package com.cisco.commons.processing.distributed.etcd;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.cisco.commons.processing.DataObject;
import com.cisco.commons.processing.DataObjectProcessResultHandler;
import com.cisco.commons.processing.DataObjectProcessor;
import com.cisco.commons.processing.DataProcessor;
import com.cisco.commons.processing.retry.FailureHandler;
import com.google.gson.Gson;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Lock;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lock.LockResponse;
import io.etcd.jetcd.lock.UnlockResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.GetOption.SortOrder;
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
public class ETCDDataProcessor extends DataProcessor {

	private static final int REQUEST_TIMEOUT_SECONDS = 30;
	private static final String PENDING = "pending.";
	private static final String INPROGRESS = "inprogress.";
	private static final String QUEUE_MAP_PREFIX = "commons-processing-etcd.queue.map.";
	private static final String QUEUE_MAP_TIMESTAMP_PENDING_PREFIX = QUEUE_MAP_PREFIX + "timestamp." + PENDING;
	private static final String QUEUE_MAP_TIMESTAMP_INPROGRESS_PREFIX = QUEUE_MAP_PREFIX + "timestamp." + INPROGRESS;
	private static final String QUEUE_MAP_DATA_PREFIX = QUEUE_MAP_PREFIX + "data.";
	public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
	private static final long MAX_DATA_OBJECT_PROCESSING_TIME_SECONDS = 60 * 5;
	private RandomGenerator randomGenerator;
	private Gson gson;
	private KV kvClient;
	private Lock lockClient;
	private Lease leaseClient;
	private ByteSequence queueMapLock;
	private ByteSequence queueMapTimestampePendingPrefixByteSeq;
	private ByteSequence queueMapTimestampeInprogressPrefixByteSeq;
	private ByteSequence queueMapDataPrefixByteSeq;
	
	@Getter
	private Client client;

	public static ETCDDataProcessorBuilder newBuilder() {
		return new ETCDDataProcessorBuilder();
	}

	public ETCDDataProcessor(Integer numOfThreads, Long retryDelay, TimeUnit retryDelayTimeUnit, int retries,
			DataObjectProcessor dataObjectProcessor, DataObjectProcessResultHandler dataObjectProcessResultHandler,
			FailureHandler failureHandler, boolean shouldAggregateIfAlreadyRunning, String etcdUrl, Client client,
			RandomGenerator randomGenerator) {
		super(numOfThreads, retryDelay, retryDelayTimeUnit, retries, dataObjectProcessor, dataObjectProcessResultHandler, failureHandler, shouldAggregateIfAlreadyRunning);
		gson = new Gson();
		this.client = client;
		if (this.client == null) {
			this.client = Client.builder().endpoints(etcdUrl).build();
		}
		this.randomGenerator = randomGenerator;
		if (this.randomGenerator == null) {
			this.randomGenerator = new UUIDRandomGenerator();
		}
		kvClient = client.getKVClient();
		lockClient = client.getLockClient();
		leaseClient = client.getLeaseClient();
		queueMapTimestampePendingPrefixByteSeq = ByteSequence.from(QUEUE_MAP_TIMESTAMP_PENDING_PREFIX, DEFAULT_CHARSET);
		queueMapTimestampeInprogressPrefixByteSeq = ByteSequence.from(QUEUE_MAP_TIMESTAMP_INPROGRESS_PREFIX, DEFAULT_CHARSET);
		queueMapDataPrefixByteSeq = ByteSequence.from(QUEUE_MAP_DATA_PREFIX, DEFAULT_CHARSET);
		queueMapLock = ByteSequence.from(QUEUE_MAP_PREFIX, DEFAULT_CHARSET);
	}

	@Override
	protected void addDataObject(DataObject dataObject) throws Exception {
		try {
			lockMap();
			String dataObjectMapKeyStr = QUEUE_MAP_DATA_PREFIX + dataObject.getKey().toString();
			byte[] dataObjectKeyBytes = dataObjectMapKeyStr.getBytes(DEFAULT_CHARSET);
			ByteSequence dataObjectKeyBytesSeq = ByteSequence.from(dataObjectKeyBytes);
			ByteSequence dataObjectDataBytesSeq = buildByteSeq(dataObject);
			PutResponse valuePutResponse = kvClient.put(dataObjectKeyBytesSeq, dataObjectDataBytesSeq)
				.get(30, TimeUnit.SECONDS);
			if (!valuePutResponse.hasPrevKv()) {
				String randomString = randomGenerator.generateRandomString();
				String dataObjectMapTimestampKeyStr = QUEUE_MAP_TIMESTAMP_PENDING_PREFIX + 
						System.currentTimeMillis() + "." + randomString;

				byte[] dataObjectTimestampKeyBytes = dataObjectMapTimestampKeyStr.getBytes(DEFAULT_CHARSET);
				ByteSequence dataObjectTimestampKeyBytesSeq = ByteSequence.from(dataObjectTimestampKeyBytes);

				PutResponse timestampPutResponse = kvClient.put(dataObjectTimestampKeyBytesSeq, dataObjectKeyBytesSeq)
					.get(30, TimeUnit.SECONDS);
				log.info("addDataObject prevKv: {}", timestampPutResponse.getPrevKv());
			}
		} finally {
			unlockMap();
		}
	}

	private ByteSequence buildByteSeq(DataObject dataObject) {
		String dataObjectDataStr = gson.toJson(dataObject);
		return ByteSequence.from(dataObjectDataStr, DEFAULT_CHARSET);
	}
	
	private void lockMap() throws InterruptedException, ExecutionException, TimeoutException {
		CompletableFuture<LeaseGrantResponse> leaseFuture = leaseClient.grant(MAX_DATA_OBJECT_PROCESSING_TIME_SECONDS);
		LeaseGrantResponse leaseResponse = leaseFuture.get(30, TimeUnit.SECONDS);
		long leaseId = leaseResponse.getID();
		CompletableFuture<LockResponse> lockFuture = lockClient.lock(queueMapLock, leaseId);
		LockResponse lockResponse = lockFuture.get(MAX_DATA_OBJECT_PROCESSING_TIME_SECONDS, TimeUnit.SECONDS);
		log.debug("lockResponse key: {}", lockResponse.getKey());
	}
	
	private void unlockMap() throws Exception {
		UnlockResponse unlockResponse = lockClient.unlock(queueMapLock).get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
		log.debug("unlockResponse header: {}", unlockResponse.getHeader());
	}

	@Override
	protected DataObject poll() throws Exception {
		try {
			lockMap();
			GetOption getOption = GetOption.newBuilder().withPrefix(queueMapTimestampePendingPrefixByteSeq)
				.withSortOrder(SortOrder.ASCEND).withLimit(1).build();
			CompletableFuture<GetResponse> getFuture = kvClient.get(queueMapTimestampePendingPrefixByteSeq, getOption);
			GetResponse response = getFuture.get(30, TimeUnit.SECONDS);
			List<KeyValue> kvs = response.getKvs();
			if (!kvs.isEmpty()) {
				KeyValue kv = kvs.iterator().next();
				ByteSequence pendingKeyBytesSeq = kv.getKey();
				ByteSequence valueBytesSeq = kv.getValue();
				replacePendingWithInprogress(pendingKeyBytesSeq, valueBytesSeq);
				CompletableFuture<GetResponse> getDataFuture = kvClient.get(valueBytesSeq,
					getOption);
				GetResponse responseData = getDataFuture.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
				List<KeyValue> kvsData = responseData.getKvs();
				if (!kvsData.isEmpty()) {
					KeyValue dataKV = kvsData.iterator().next();
					ByteSequence dataValueBytesSeq = dataKV.getValue();
					String dataValue = dataValueBytesSeq.toString(DEFAULT_CHARSET);
					return gson.fromJson(dataValue, DataObject.class);
				}
			}
			return null;
		} finally {
			unlockMap();
		}
	}

	private void replacePendingWithInprogress(ByteSequence pendingKeyBytesSeq, ByteSequence valueBytesSeq)
			throws InterruptedException, ExecutionException, TimeoutException {
		String pendingKeyStr = pendingKeyBytesSeq.toString(DEFAULT_CHARSET);
		String inProgressKeyStr = pendingKeyStr.replace(QUEUE_MAP_TIMESTAMP_PENDING_PREFIX, 
			QUEUE_MAP_TIMESTAMP_INPROGRESS_PREFIX);
		byte[] inProgressKeyBytes = inProgressKeyStr.getBytes(DEFAULT_CHARSET);
		ByteSequence inProgressKeyBytesSeq = ByteSequence.from(inProgressKeyBytes);
		PutResponse valuePutResponse = kvClient.put(inProgressKeyBytesSeq, valueBytesSeq)
			.get(30, TimeUnit.SECONDS);
		log.info("replacePendingWithInprogress valuePutResponse: {}", valuePutResponse.getPrevKv());
		CompletableFuture<DeleteResponse> deleteResponseFuture = kvClient.delete(pendingKeyBytesSeq);
		DeleteResponse deleteResponse = deleteResponseFuture.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
		log.debug("replacePendingWithInprogress deleteResponse deleted: {}", deleteResponse.getDeleted());
	}
	
	@Override
	protected void postProcess(DataObject dataObject) throws Exception {
		
		try {
			lockMap();
			String dataObjectMapKeyStr = QUEUE_MAP_DATA_PREFIX + dataObject.getKey().toString();
			ByteSequence dataObjectKeyBytesSeq = ByteSequence.from(dataObjectMapKeyStr, DEFAULT_CHARSET);
			DeleteResponse deleteResponse = kvClient.delete(dataObjectKeyBytesSeq)
				.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			log.debug("postProcess deleteResponse deleted: {}", deleteResponse.getDeleted());
			
			// TODO delete also the inprogress key from timestamp map
			
		} finally {
			unlockMap();
		}
	}

	@Override
	public void shutdown() {
		try {
			super.shutdown();
		} catch (Exception e) {
			log.error("Error shutdown." + e.getMessage());
		}
	}
}
