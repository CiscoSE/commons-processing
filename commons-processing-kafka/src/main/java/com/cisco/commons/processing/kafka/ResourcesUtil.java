package com.cisco.commons.processing.kafka;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.sun.management.OperatingSystemMXBean;

/**
 * Resources utilities.
 * 
 * Resources like CPU and memory.
 * 
 * @author Liran Mendelovich
 *
 */
public class ResourcesUtil {
	
	private ResourcesUtil() {
		
	}
	
	/**
	 * Calculate resources statistics.
	 * 
	 * @return map with the values. 
	 */
	public static Map<String, Object> calculateResourcesStats() {
		Map<String, Object> stats = new LinkedHashMap<>();
		MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
		MemoryUsage nmu = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
		long currentMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		stats.put("uptime.hours", getUpTimeHours());
		stats.put("max.memory.usage", String.valueOf(Runtime.getRuntime().maxMemory()));
		stats.put("current.memory.usage", String.valueOf(currentMemory));
		stats.put("heap.memory.usage", mu.toString());
		stats.put("non.heap.memory.usage", nmu.toString());
		stats.putAll(calculateCPUStats("app"));
		return Collections.unmodifiableMap(stats);
	}
    
    private static String getUpTimeHours() {
    	return String.valueOf(TimeUnit.MILLISECONDS.toHours(ManagementFactory.getRuntimeMXBean().getUptime()));
	}

	private static Map<String, String> calculateCPUStats(String label) {
		Map<String, String> stats = new LinkedHashMap<>();
		OperatingSystemMXBean operatingSystemMXBean = 
            (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
		stats.put(label + ".available.processors", String.valueOf(operatingSystemMXBean.getAvailableProcessors()));
		stats.put(label + ".free.physical.memory", String.valueOf(operatingSystemMXBean.getFreePhysicalMemorySize()));
		stats.put(label + ".process.cpu.load", String.valueOf(operatingSystemMXBean.getProcessCpuLoad()));
		stats.put(label + ".process.cpu.time", String.valueOf(operatingSystemMXBean.getProcessCpuTime()));
		stats.put(label + ".system.cpu.load", String.valueOf(operatingSystemMXBean.getSystemCpuLoad()));
		stats.put(label + ".system.load.average", String.valueOf(operatingSystemMXBean.getSystemLoadAverage()));
		return Collections.unmodifiableMap(stats);
	}
}
