package com.cisco.commons.processing.retry;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.Test;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RetryExecutorTest {

    @Test
    public void retryExecutorTest() throws InterruptedException {
    	log.info("retryExecutorTest begin");
		RetryExecutor retryExecutor = RetryExecutor.builder()
    		.build();
        ExecutorService pool = Executors.newCachedThreadPool();
        AtomicInteger count = new AtomicInteger(0);
        Supplier<Boolean> supplier = () -> {
            count.incrementAndGet();
            return false;
        };
        int retryDelaySeconds = 1;
        int retries = 1;
        AtomicInteger resultsCount = new AtomicInteger(0);
        ResultHandler resultHandler = new ResultHandler() {
			@Override
			public void handleResult(Supplier<Boolean> supplier, Boolean result) {
				resultsCount.incrementAndGet();
			}
		};
		retryExecutor.executeAsync(supplier, pool, retryDelaySeconds, TimeUnit.SECONDS, retries, resultHandler, null);
		Thread.sleep(retryDelaySeconds * 1000 + 500);
        assertEquals(1 + retries, count.intValue());
        assertEquals(1, resultsCount.intValue());
        
        BackOff backOff = new ExponentialBackOff.Builder()
    		.setInitialIntervalMillis(500)
    		.setMultiplier(1.5)
    		.setMaxElapsedTimeMillis(Integer.MAX_VALUE)
    		.build();
        
        retryExecutor.executeAsync(supplier, pool, backOff, retries, resultHandler, null);

        log.info("retryExecutorTest end");
    }

}
