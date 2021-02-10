package com.cisco.commons.processing.retry;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

/**
 * RetryExecutor
 * Provide ability to execute async tasks with a delayed retry mechanism, without blocking for waiting for a response.
 * It is getting a provided pool for the tasks, and using an internal scheduled pool for retries scheduling only.
 * close() must be closed when done.
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
public class RetryExecutor {

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
    	executeAsync(supplier, pool, delay, timeUnit, retries, 0, resultHandler, failureHandler);
    }

    private void executeAsync(Supplier<Boolean> supplier, ExecutorService pool, long delay, TimeUnit timeUnit, int retries, int count, ResultHandler resultHandler, FailureHandler failureHandler) {
        log.debug("executeAsync called, count: {}", count);
        BiConsumer<? super Boolean, ? super Throwable> action = (result, throwable) -> 
        	whenComplete(supplier, result, throwable, pool, delay, timeUnit, retries, count, resultHandler, failureHandler);
        long delayValue = delay;
        if (count == 0) {
        	delayValue = 0;
        }
        Executor executor = CompletableFuture.delayedExecutor(delayValue, timeUnit, pool);
        log.debug("Scheduling delayed execution for {} {} from now.", delayValue, timeUnit);
        CompletableFuture.supplyAsync(supplier, executor).whenCompleteAsync(action, pool);
    }

    private void whenComplete(Supplier<Boolean> supplier, Boolean result, Throwable throwable, ExecutorService pool, 
    		long delay, TimeUnit timeUnit, int retries, int count, ResultHandler resultHandler, FailureHandler failureHandler) {
        log.debug("whenComplete called with result: {}, count: {}, retries: {}", result, count, retries);
        log.debug("throwable: ", throwable);
        if (!result && count < retries) {
            log.debug("Execution failed. Scheduling retry execution.");
            executeAsync(supplier, pool, delay, timeUnit, retries, count + 1, resultHandler, failureHandler);
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

    public void setRetriesScheduledPool(ExecutorService retriesScheduledPool) {
        this.retriesScheduledPool = retriesScheduledPool;
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
