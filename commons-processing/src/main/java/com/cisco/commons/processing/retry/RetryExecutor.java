package com.cisco.commons.processing.retry;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import com.google.api.client.util.BackOff;

import lombok.Builder;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * RetryExecutor
 * Provide ability to execute async tasks with a delayed retry mechanism, without blocking for waiting for a response.
 * It is getting a provided pool for the tasks, and using an internal scheduled pool for retries scheduling only.
 * close() must be closed when done.
 * 
 * <br/>
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
 * 
 * @author Liran Mendelovich
 */
@Slf4j
public class RetryExecutor {

	@Setter
    private ExecutorService retriesScheduledPool;
    
    @Builder
    private RetryExecutor() {
    	retriesScheduledPool = Executors.newCachedThreadPool();
    }

    /**
     * Execute async tasks with a delayed retry mechanism, without blocking for waiting for a response.
     * @param supplier - supplier for the action, should return the boolean result of the action, true indicates success, and false indicates on a failure, resulting in conditional retry.
     * @param pool - the pool to execute the tasks.
     * @param delay - delay for retrying failing actions.
     * @param timeUnit delay time unit.
     * @param retries - number of times for retries attempts, not including the initial action.
     * @param resultHandler - optional last result handler - called when all attempts complete.
     * @param failureHandler - optional failure handler - called when all attempts complete with failure.
     */
    public void executeAsync(Supplier<Boolean> supplier, ExecutorService pool, long delay, TimeUnit timeUnit, int retries, ResultHandler resultHandler, FailureHandler failureHandler) {
    	BackOff backOff = new BackOff() {
			
			@Override
			public void reset() throws IOException {
				
			}
			
			@Override
			public long nextBackOffMillis() throws IOException {
				return timeUnit.toMillis(delay);
			}
		};
    	executeAsync(supplier, pool, backOff, retries, resultHandler, failureHandler);
    }
    
    /**
     * Execute async tasks with a delayed retry mechanism, without blocking for waiting for a response.
     * @param supplier - supplier for the action, should return the boolean result of the action, true indicates success, and false indicates on a failure, resulting in conditional retry.
     * @param pool - the pool to execute the tasks.
     * @param backOff - BackOff to use for delay time
     * @param retries - number of times for retries attempts, not including the initial action.
     * @param resultHandler - optional last result handler - called when all attempts complete.
     * @param failureHandler - optional failure handler - called when all attempts complete with failure.
     */
    public void executeAsync(Supplier<Boolean> supplier, ExecutorService pool, BackOff backOff, 
    		int retries, ResultHandler resultHandler, FailureHandler failureHandler) {
    	executeAsync(supplier, pool, backOff, retries, 0, resultHandler, failureHandler);
    }

    private void executeAsync(Supplier<Boolean> supplier, ExecutorService pool, BackOff backOff, int retries, int count, ResultHandler resultHandler, FailureHandler failureHandler) {
        log.debug("executeAsync called, count: {}", count);
        BiConsumer<? super Boolean, ? super Throwable> action = (result, throwable) -> 
        	whenComplete(supplier, result, throwable, pool, backOff, retries, count, resultHandler, failureHandler);
        long delayValue = 0;
        if (count > 0) {
        	delayValue = nextBackOffMillis(backOff);
        }
        Executor executor = CompletableFuture.delayedExecutor(delayValue, TimeUnit.MILLISECONDS, pool);
        String messageFormat = "Scheduling delayed execution for {} ms from now.";
        if (delayValue > 0) {
        	log.info(messageFormat, delayValue);
        } else {
        	log.debug(messageFormat, delayValue);
        }
        CompletableFuture.supplyAsync(supplier, executor).whenCompleteAsync(action, pool);
    }

    private void whenComplete(Supplier<Boolean> supplier, Boolean result, Throwable throwable, ExecutorService pool, 
    		BackOff backOff, int retries, int count, ResultHandler resultHandler, FailureHandler failureHandler) {
        log.debug("whenComplete called with result: {}, count: {}, retries: {}", result, count, retries);
        log.debug("throwable: ", throwable);
        if (!result && count < retries) {
            log.info("Execution failed. Scheduling retry execution.");
            executeAsync(supplier, pool, backOff, retries, count + 1, resultHandler, failureHandler);
        } else {
        	log.debug("Execution done.");
        	if (resultHandler != null) {
        		handleResult(supplier, resultHandler, result);
        	}
        	if (!result && failureHandler != null) {
        		handleFailure(supplier, failureHandler);
        	}
        }
    }

	private void handleFailure(Supplier<Boolean> supplier, FailureHandler failureHandler) {
		try {
			failureHandler.handleFailure(supplier);
		} catch (Exception e) {
			log.error("Error in failureHandler handleFailure: " + e.getMessage(), e);
		}
	}

	private void handleResult(Supplier<Boolean> supplier, ResultHandler resultHandler, Boolean result) {
		try {
			resultHandler.handleResult(supplier, result);
		} catch (Exception e) {
			log.error("Error in resultHandler handleResult: " + e.getMessage(), e);
		}
	}
    
    public static long nextBackOffMillis(BackOff backoff) {
		try {
			return backoff.nextBackOffMillis();
		} catch (IOException e) {
			log.error("Error getting nextBackOffMillis: " + e.getClass() + ", " + e.getMessage(), e);
			return 0;
		}
	}

    /**
     * Shutdown the internal retries scheduled pool.
     */
    public void close() {
        try {
			retriesScheduledPool.shutdown();
		} catch (Exception e) {
			log.error("Error closing: " + e.getMessage(), e);
		}
    }

}
