package com.cisco.commons.processing.retry;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.Test;

public class RetryExecutorTest {

    @Test
    public void retryExecutorTest() throws InterruptedException {
        RetryExecutor retryExecutor = RetryExecutor.builder().build();
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
    }

}
