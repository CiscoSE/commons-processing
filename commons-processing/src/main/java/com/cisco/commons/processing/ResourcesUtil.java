package com.cisco.commons.processing;

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
 * Utility statistics methods related to resources like CPU and memory.
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
 *
 */
public class ResourcesUtil {
	
	private ResourcesUtil() {
		
	}
	
	/**
	 * <pre>
	 * Calculate resources statistics.
	 * Implementation is largely based on Java APIs, ManagementFactory and Runtime.
	 * Some reference for heap memory explanation:
	 * <a href="https://stackoverflow.com/questions/3571203/what-are-runtime-getruntime-totalmemory-and-freememory">Runtime memory</a>
	 * 
	 * Memory diagram:
	 * =====================================================
	 *                              Java process memory    =
	 * =====================================================
	 * =                   heap            =  non-heap     =
	 * =====================================================
	 * =                   max             =   committed   =
	 * ===================                 =================
	 * =       total     =                 =   used     =  =
	 * =                 ===================================
	 * =   used    free  =    unallocated  =               =
	 * =====================================================
	 * 
	 * Example result when running ResourcesUtilTest.calculateResourcesStatsTest with size=10000000:
	 * {
	 *  "uptime.hours": "0",
	 *  "total.memory.usage": "1600126976",
	 *  "max.memory.usage": "4263510016",
	 *  "used.memory.usage": "622856192",
	 *  "free.memory": "977270784",
	 *  "free.total.memory": "3640653824",
	 *  "memory.usage.percentage": "14",
	 *  "heap.memory.usage": "init \u003d 268435456(262144K) used \u003d 622329856(607744K) committed \u003d 1600126976(1562624K) max \u003d 4263510016(4163584K)",
	 *  "non.heap.memory.usage": "init \u003d 7667712(7488K) used \u003d 25165856(24576K) committed \u003d 29360128(28672K) max \u003d -1(-1K)",
	 *  "app.available.processors": "8",
	 *  "app.free.physical.memory": "3274403840",
	 *  "app.process.cpu.load": "0.24405113958215321",
	 *  "app.process.cpu.time": "5593750000",
	 *  "app.system.cpu.load": "0.5908797653958944",
	 *  "app.system.load.average": "-1.0"
	 * }
	 * </pre>
	 * 
	 * @return map with the values. 
	 */
	public static Map<String, Object> calculateResourcesStats() {
		Map<String, Object> stats = new LinkedHashMap<>();
		MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
		MemoryUsage nmu = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
		long totalMemory = Runtime.getRuntime().totalMemory();
		long maxMemory = Runtime.getRuntime().maxMemory();
		long freeMemory = Runtime.getRuntime().freeMemory();
		long usedMemory = totalMemory - freeMemory;
		long freeTotalMemory = maxMemory - usedMemory;
		int usedMemoryPercentage = (int)(((double)usedMemory / maxMemory) * 100);
		stats.put("uptime.hours", getUpTimeHours());
		stats.put("total.memory.usage", String.valueOf(totalMemory));
		stats.put("max.memory.usage", String.valueOf(maxMemory));
		stats.put("used.memory.usage", String.valueOf(usedMemory));
		stats.put("free.memory", String.valueOf(freeMemory));
		stats.put("free.total.memory", String.valueOf(freeTotalMemory));
		stats.put("memory.usage.percentage", String.valueOf(usedMemoryPercentage));
		stats.put("heap.memory.usage", mu.toString());
		stats.put("non.heap.memory.usage", nmu.toString());
		stats.putAll(calculateCPUStats("app"));
		
		// Commented for now, as UnixOperatingSystemMXBean import is not recommended.
//		OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
//        if(os instanceof UnixOperatingSystemMXBean){
//        	stats.put("open.file.descriptors", String.valueOf(((UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount()));
//        }
		
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
