package com.cisco.commons.processing.kafka;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Topic consumer handler for processing messages in parallel.
 * Parallelism is by best effort without using all resources by running each bulk in parallel, as target is to execute 
 * Kafka commit only when each bulk processing is done, for maintaining Kafka topic consumer offset.
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
public class ParallelConsumerHandler implements ConsumerHandler {

	private ExecutorService processingPool;
	
	@Setter
	private MessageProcessor messageProcessor;
	
	@Setter
	private Integer consumerProcesingPoolSize;
	
	@Setter
	private Integer bulkProcessingTimeoutSeconds = 60;
	
	@Setter
	private Integer poolShutdownTimeoutSeconds = 10;
	
	private AtomicBoolean shouldRun = new AtomicBoolean(true);

	public ParallelConsumerHandler() {
		Objects.requireNonNull(messageProcessor);
		Objects.requireNonNull(consumerProcesingPoolSize);
		this.processingPool = Executors.newFixedThreadPool(consumerProcesingPoolSize);
	}
	
	@Override
	public void handle(ConsumerRecords<String, byte[]> kafkaRecords) {
		try {
			Collection<Callable<Boolean>> processingTasks = new LinkedList<>();
			int recordsCount = 0;
			for (ConsumerRecord<String, byte[]> kafkaRecord: kafkaRecords) {
				processingTasks.add(() -> {messageProcessor.process(kafkaRecord); return true;});
				recordsCount++;
			}
			List<Future<Boolean>> processorsResponses = processingPool.invokeAll(processingTasks, bulkProcessingTimeoutSeconds, TimeUnit.SECONDS);
			int completedCount = 0;
			for (Future<Boolean> processorFuture : processorsResponses) {
				if (processorFuture.isDone() && !processorFuture.isCancelled()) {
					completedCount++;
				}
			}
			boolean isAllDone = completedCount == recordsCount;
			log.info("processed {} records, isAllDone: {}, tasks completed: {}", recordsCount, isAllDone, completedCount);
			if (shouldRun.get() && !isAllDone) {
				log.error("Not all tasks finished up to the timeout.");
			}
		} catch (InterruptedException e) {
			log.info("interrupted.");
		}
	}
	
	public void shutdown() {
		try {
			log.info("shutdown");
			shouldRun.set(false);
			ConcurrentUtils.shutdownAndAwaitTermination(processingPool, poolShutdownTimeoutSeconds);
		} catch (Exception e) {
			log.error("Error shutdown: " + e.getClass().getSimpleName() + ": " + e.getMessage(), e);
		}
	}
	
}
