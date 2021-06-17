package com.cisco.commons.processing.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConfigEntry.ConfigSource;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;

import lombok.extern.slf4j.Slf4j;

/**
 * Kafka utilities.
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
public class KafkaUtils {
	
	private KafkaUtils() {
		
	}
	
	/**
	 * Set dynamic topic configuration if not already set.
	 * 
	 * Example usage: <br/>
	 * <code>
	 * Map<String, String> configMap = new HashMap<>();
	 * configMap.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(Duration.ofHours(6).toMillis()));
	 * configMap.put(TopicConfig.RETENTION_BYTES_CONFIG, Long.toString(1024 * 1024 * 500));
	 * KafkaUtils.setDynamicTopicConfig(applicationConfig.getKafkaUrl(), applicationConfig.getDataGetTopic(), configMap);
	 * </code>
	 * 
	 * @param kafkaUrl - Kafka URL
	 * @param topic - Kafka topic
	 * @param configMapCandidate - configuration map candidate to update if needed
	 * @throws Exception - in case of error
	 */
	public static void setDynamicTopicConfig(String kafkaUrl, String topic,
			Map<String, String> configMapCandidate) throws Exception {
		log.info("Setting topic {} config map candidate: {}", topic, configMapCandidate);
		Map<String, Object> config = new HashMap<>();                
	    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
	    AdminClient client = AdminClient.create(config);
	    Map<String, String> configMap = new HashMap<>(configMapCandidate);
	    ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
	    DescribeConfigsResult topicsConfig = client.describeConfigs(Arrays.asList(resource));
	    KafkaFuture<Map<ConfigResource, Config>> mapFuture = topicsConfig.all();
	    if (mapFuture != null) {
	    	Map<ConfigResource, Config> topicMap = mapFuture.get(3, TimeUnit.SECONDS);
	    	if (topicMap != null) {
	    		Set<Entry<ConfigResource, Config>> entries = topicMap.entrySet();
	    		for (Entry<ConfigResource, Config> entry : entries) {
					updateConfigMap(configMapCandidate, configMap, entry);
				}
	    	}
	    }
	    if (!config.isEmpty()) {
	    	setTopicConfig(client, topic, resource, configMap);
	    } else {
	    	log.info("Not setting config as not needed for topic: {}", topic);
	    }
	}

	private static void setTopicConfig(AdminClient client, String topic, ConfigResource resource,
			Map<String, String> configMap) {
		log.info("Setting topic: {} config: {}", topic, configMap);
		Map<ConfigResource, Config> updateConfig = new HashMap<>();
		List<ConfigEntry> configList = new ArrayList<>(configMap.size());
		List<AlterConfigOp> operations = new ArrayList<>(configMap.size());
		Set<Entry<String, String>> configMapEntries = configMap.entrySet();
		for (Entry<String, String> entry : configMapEntries) {
			ConfigEntry configEntry = new ConfigEntry(entry.getKey(), entry.getValue());
			configList.add(configEntry);
			AlterConfigOp op = new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET);
			operations.add(op);
		}
		updateConfig.put(resource, new Config(configList));
		Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
		configs.put(resource, operations);
		AlterConfigsResult alterConfigsResult = client.incrementalAlterConfigs(configs);
		alterConfigsResult.all();
	}

	private static void updateConfigMap(Map<String, String> configMapCandidate, Map<String, String> configMap,
			Entry<ConfigResource, Config> entry) {
		Config entryConfig = entry.getValue();
		Set<Entry<String, String>> configMapEntries = configMapCandidate.entrySet();
		for (Entry<String, String> configMapEntry : configMapEntries) {
			ConfigEntry kafkaConfigEntry = entryConfig.get(configMapEntry.getKey());
			log.info("Existing kafkaConfigEntry: {}", kafkaConfigEntry);
			if (kafkaConfigEntry != null &&
					ConfigSource.DYNAMIC_TOPIC_CONFIG == kafkaConfigEntry.source() && 
					Objects.equals(kafkaConfigEntry.value(), configMapEntry.getValue())) {
				log.info("{} already equals {}, not setting new value.", configMapEntry.getKey(), configMapEntry.getValue());
				configMap.remove(configMapEntry.getKey());
			}
		}
	}
	
	public static void requireTrue(boolean expression, String message) {
		if (!expression) {
			throw new IllegalArgumentException(message);
		}
	}
}
