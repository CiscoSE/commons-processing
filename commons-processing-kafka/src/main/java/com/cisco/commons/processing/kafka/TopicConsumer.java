package com.cisco.commons.processing.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Kafka topic consumer.
 * 
 * Intended to provider some parallelism option and bulk processing for Kafka topic, 
 * while maintaining the topic offset.
 * Parallelism and bulk processing can be beneficial to processing like database related actions performance.
 * 
 * Implemented by polling in bulks, while each bulk can be processed in parallel if wanted by consumer handler, with
 * Kafka commit triggering only after each bulk is processed.
 * The consumer has a sync retry mechanism.
 * 
 * Working by "at least once" message processing paradigm, which is mostly the case anyway at common consumers implementation,
 * as message processing can fail before the Kafka commit action. Thus the consumer handler is expected to treat
 * each message with idempotent outcome.
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
public class TopicConsumer {

	@Setter @Getter
	private Duration pollTimeout;
	
	@Setter @Getter
	private String name;
	
	@Setter
	private String topicName;
	
	@Setter
	private String groupId;
	
	@Setter
	private Integer maxPollRecords;
	
	@Setter
	private Properties consumerProperties;
	
	@Setter
	private ConsumerHandler consumerHandler;
	
	@Setter
    private String kafkaUrl;
	
	/*
	 * For topic which expects high probability of multiple notifications on same objects, delay can be
	 * before the polling itself, thus increasing the probability of polling the related notifications in same
	 * bulk.
	 * consumerHandler can process the bulk, and populate it to a map by a logic key to merge duplicated notifications.
	 */
	@Setter
	private boolean delayBeforePolling = false;
	
	private Consumer<String, byte[]> consumer;
	private AtomicBoolean shouldRun = new AtomicBoolean(true);
	private CountDownLatch liveCountDownLatch = new CountDownLatch(1);
	private AtomicBoolean isInitialized = new AtomicBoolean(false);
	private AtomicBoolean isStarted = new AtomicBoolean(false);
	private ExecutorService consumerPool;

	@Setter
	private static int delaySeconds = 30;
	
	public TopicConsumer() {
		super();
	}
	
	public void init() {
		Objects.requireNonNull(pollTimeout, "pollTimeout is not set");
		Objects.requireNonNull(name, "name is not set");
		Objects.requireNonNull(topicName, "topicName is not set");
		Objects.requireNonNull(kafkaUrl, "kafkaUrl is not set");
		Objects.requireNonNull(groupId, "groupId is not set");
		Objects.requireNonNull(maxPollRecords, "maxPollRecords is not set");
		if (consumerProperties == null) {
			log.debug("consumerProperties is not set. Using default properties.");
			consumerProperties = createDefaultProperties();
		}
		Objects.requireNonNull(consumerHandler, "consumerHandler is not set");
		consumerPool = Executors.newSingleThreadExecutor();
		log.info("init. name: {}, topicName: {}, pollTimeout: {}, consumerProperties: {}", name, topicName, pollTimeout, consumerProperties);
		isInitialized.set(true);
	}
	
	public Properties createDefaultProperties() {
		Properties consumerProperties = new Properties();
		consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
	    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
	    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
	    consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
	    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	    consumerProperties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(Duration.ofMinutes(5).toMillis()));
	    consumerProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(Duration.ofSeconds(30).toMillis()));
	    consumerProperties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(Duration.ofSeconds(3).toMillis()));
	    consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
	    return consumerProperties;
	}

	public void startConsumingAsync() {
		log.info("startConsumingAsync");
		KafkaUtils.requireTrue(isInitialized.get(),"not initialized.");
		shouldRun.set(true);
		if (!isStarted.compareAndSet(false, true)) {
			log.info("Already started.");
			return;
		}
		liveCountDownLatch = new CountDownLatch(1);
		subscribe();
		Runnable consumerTask = () -> {
			startConsumingSync();
		};
		consumerPool.execute(consumerTask);
	}

	private void subscribe() {
		boolean isSubscribeSucceed = false;
		while (shouldRun.get() && !isSubscribeSucceed) {
			try {
				consumer = new KafkaConsumer<>(consumerProperties);
				consumer.subscribe(Arrays.asList(topicName));
				isSubscribeSucceed = true;
			} catch (Exception e) {
				log.error(name + "Error subscribing to topic: " + topicName + ", got error: " + e.getMessage() + 
					", will retry after delay");
				isSubscribeSucceed = false;
				try {
					TimeUnit.SECONDS.sleep(30);
				} catch (InterruptedException e1) {
					log.error(name + "Error delaying before resubscribing to topic: " + topicName + ", got error: " 
				+ e.getMessage() + " stopping consumer.");
					shouldRun.set(false);
				}
			}
		}
	}
	
	private void startConsumingSync() {
		KafkaUtils.requireTrue(isInitialized.get(),"not initialized.");
        log.info(name + " startConsumingSync begin");
        while (shouldRun.get()) {
        	ConsumerRecords<String, byte[]> kafkaRecords = null;
			try {
				kafkaRecords = poll();
			} catch (Exception e) {
				if (!shouldRun.get()) {
					log.debug("shouldRun is false. Got exception: " + e.getClass() + ", " + e.getMessage());
				} else {
					handlePollingError(e);
				}
			}
    		if ( kafkaRecords == null || isEmpty(kafkaRecords) ) {
    			log.debug(name + " no new records");
            	continue;
    		}
    		if (!shouldRun.get()) {
    			log.debug(name + " stopping after polling");
            	break;
    		}
    		log.debug("Polled records.");
    		handle(kafkaRecords);
			log.debug(name + " consumer committing");
    		commitSync();
    		log.debug(name + " consumer committed");
        }
        log.info(name + " startConsumingSync end");
	}

	private void handlePollingError(Exception e) {
		log.error(name + " Error polling from Kafka topic: " + topicName + ", got error: " + e.getMessage() + " Delaying.");
		try {
			TimeUnit.SECONDS.sleep(delaySeconds);
		} catch (InterruptedException e1) {
			if (shouldRun.get()) {
				log.error(name + " Error delaying before polling from topic: " + topicName + ", got error: " 
			+ e1.getMessage() + " stopping consumer.");
				shouldRun.set(false);
			} else {
				log.debug(name + " Error delaying before polling from topic: {}", topicName);
			}
		}
	}
	
	private void handle(ConsumerRecords<String, byte[]> kafkaRecords) {
		int retries = 1;
		int count = 0;
		boolean success = false;
		while (shouldRun.get() && count <= retries && !success) {
    		try {
				consumerHandler.handle(kafkaRecords);
				success = true;
				log.debug("Processed messages.");
			} catch (Exception e) {
				log.error("Failed processing entities: " + e.getMessage(), e);
				if (count < retries) {
					log.info("Delaying before retry.");
					delay(10);
				}
			}
			count++;
		}
		if (!success) {
			log.error("Failed processing entities after retry: {}", kafkaRecords);
		}
	}

	private void delay(long seconds) {
		try {
			liveCountDownLatch.await(seconds, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			log.info("Interrupted delay");
		}
	}

	public void commitSync() {
		consumer.commitSync();
	}

	public boolean isEmpty(ConsumerRecords<String, byte[]> kafkaRecords) {
		return kafkaRecords.isEmpty();
	}

	public ConsumerRecords<String, byte[]> poll() {
		if (delayBeforePolling) {
			delay(pollTimeout.getSeconds());
			if (shouldRun.get()) {
				return consumer.poll(Duration.ofMillis(1));
			} else {
				return null;
			}
		} else {
			return consumer.poll(pollTimeout);
		}
	}
	
	public void stopConsuming() {
		log.info("stopConsuming");
		shouldRun.set(false);
		isStarted.set(false);
		try {
			long waitTime = Math.max(pollTimeout.getSeconds(), 5);
			liveCountDownLatch.await(waitTime, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			log.info("Could not await: {}", e.getMessage());
		}
		liveCountDownLatch.countDown();
		if (consumer != null) {
			try {
				consumer.unsubscribe();
			} catch (Exception e) {
				log.error("Error unsubscribe: " + e.getClass() + ": " + e.getMessage(), e);
			}
			try {
				consumer.close();
			} catch (Exception e) {
				log.error("Error close: " + e.getClass() + ": " + e.getMessage(), e);
			}
		}
	}
	
	public void shutdown() {
		shouldRun.set(false);
		stopConsuming();
		if (consumerPool != null) {
			consumerPool.shutdownNow();
		}
	}
	
}

