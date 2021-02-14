package com.cisco.commons.processing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.cisco.commons.processing.retry.FailureHandler;

import lombok.extern.slf4j.Slf4j;

@RunWith(Parameterized.class)
@Slf4j
public class DataProcessorTest {
	
	@Parameterized.Parameters
	public static Collection<Integer> numOfThreadsCollection() {
		return Arrays.asList(new Integer[] { 1, 4 });
	}
	
	private Integer numOfThreads;
	
	public DataProcessorTest(int numOfThreads) {
		this.numOfThreads = numOfThreads;
	}
	
	@Test
	public void dataProcessorTest() throws Exception {
		log.info("dataProcessorTest running with numOfThreads: {}", numOfThreads);
		int retries = 1;
		Long retryDelay = 1L;
		TimeUnit retryDelayTimeUnit = TimeUnit.SECONDS;
		String testKey = "a1";
		AtomicInteger testKeyProcessedCount = new AtomicInteger(0);
		AtomicInteger processedDataObjectsCount = new AtomicInteger(0);
		DataObjectProcessor dataObjectProcessor = (dataObject) -> {
			sleepQuitely(100);
			log.info("processed dataObject: {}", dataObject.getKey());
			processedDataObjectsCount.incrementAndGet();
			if (testKey.equals(dataObject.getKey())) {
				testKeyProcessedCount.incrementAndGet();
			}
			return true;
		};
		FailureHandler failureHandler = (supplier) -> {
			log.info("handleFailure.");
		};
		DataObjectProcessResultHandler resultHandler = new DataObjectProcessResultHandler() {
			@Override
			public void handleResult(Object dataObject, Boolean result) {
				log.info("handleResult.");
			}
		};
		DataProcessor dataProcessor = DataProcessor.builder().dataObjectProcessor(dataObjectProcessor)
				.dataObjectProcessResultHandler(resultHandler).failureHandler(failureHandler).numOfThreads(numOfThreads)
				.retries(retries).retryDelay(retryDelay).retryDelayTimeUnit(retryDelayTimeUnit).build();
		dataProcessor.aggregate(testKey, "dataObject_a1_value1");
		dataProcessor.aggregate(testKey, "dataObject_a1_value2");
		int count = numOfThreads * 3;
		log.info("count: {} ", count);
		for (int i = 0; i < count; i++) {
			dataProcessor.aggregate(String.valueOf(i), "dataObject" + i);
		}
		for (int i = 0; i < count; i++) {
			dataProcessor.aggregate(String.valueOf(i), "dataObject" + i);
		}
		for (int i = 0; i < count; i++) {
			dataProcessor.aggregate(String.valueOf(i), "dataObject" + i);
		}
		Thread.sleep(2000);
		log.info("Processed {} data objects.", processedDataObjectsCount.get());
		assertTrue(processedDataObjectsCount.get() >= count + 1 && processedDataObjectsCount.get() <= count*2 - 1);
		assertEquals(1, testKeyProcessedCount.get());
	}
	
	private void sleepQuitely(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			log.error("Error sleeping.");
		}
	}
}
