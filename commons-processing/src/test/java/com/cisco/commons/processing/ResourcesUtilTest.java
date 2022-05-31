package com.cisco.commons.processing;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ResourcesUtilTest {

	@Test
	public void calculateResourcesStatsTest() {
		log.info("calculateResourcesStatsTest begin");
		log.info("calculateResourcesStatsTest using some memory");
		
		// use higher number for seeing higher values
		int size = 100;
		
		List<String> list = new ArrayList<>(size);
		for (int i = 0; i < size; i++) {
			list.add("string" + i);
		}
		Map<String, Object> resources = ResourcesUtil.calculateResourcesStats();
		log.info("resources: {}", resources);
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		log.info("resources pretty print: {}",gson.toJson(resources));
		assertTrue("Could not calculate resources", resources.size() >= 10);
		log.info("calculateResourcesStatsTest end");
	}
	
}
