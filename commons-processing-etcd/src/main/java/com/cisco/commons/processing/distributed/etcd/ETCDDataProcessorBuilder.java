package com.cisco.commons.processing.distributed.etcd;

import java.util.concurrent.TimeUnit;

import com.cisco.commons.processing.DataObjectProcessResultHandler;
import com.cisco.commons.processing.DataObjectProcessor;
import com.cisco.commons.processing.retry.FailureHandler;

import io.etcd.jetcd.Client;
import lombok.ToString;

@ToString
public class ETCDDataProcessorBuilder {
	private Integer numOfThreads;
	private Long retryDelay;
	private TimeUnit retryDelayTimeUnit;
	private int retries;
	private DataObjectProcessor dataObjectProcessor;
	private DataObjectProcessResultHandler dataObjectProcessResultHandler;
	private FailureHandler failureHandler;
	private boolean shouldAggregateIfAlreadyRunning;
	private String etcdUrl;
	private RandomGenerator randomGenerator;
	private Client client;
	
	public ETCDDataProcessorBuilder numOfThreads(Integer numOfThreads) {
		this.numOfThreads = numOfThreads;
		return this;
	}
	
	public ETCDDataProcessorBuilder randomGenerator(RandomGenerator randomGenerator) {
		this.randomGenerator = randomGenerator;
		return this;
	}
	
	public ETCDDataProcessorBuilder etcdUrl(String etcdUrl) {
		this.etcdUrl = etcdUrl;
		return this;
	}

	public ETCDDataProcessorBuilder retryDelay(Long retryDelay) {
		this.retryDelay = retryDelay;
		return this;
	}

	public ETCDDataProcessorBuilder retryDelayTimeUnit(TimeUnit retryDelayTimeUnit) {
		this.retryDelayTimeUnit = retryDelayTimeUnit;
		return this;
	}

	public ETCDDataProcessorBuilder retries(int retries) {
		this.retries = retries;
		return this;
	}

	public ETCDDataProcessorBuilder dataObjectProcessor(DataObjectProcessor dataObjectProcessor) {
		this.dataObjectProcessor = dataObjectProcessor;
		return this;
	}

	public ETCDDataProcessorBuilder dataObjectProcessResultHandler(
			DataObjectProcessResultHandler dataObjectProcessResultHandler) {
		this.dataObjectProcessResultHandler = dataObjectProcessResultHandler;
		return this;
	}

	public ETCDDataProcessorBuilder failureHandler(FailureHandler failureHandler) {
		this.failureHandler = failureHandler;
		return this;
	}

	public ETCDDataProcessorBuilder shouldAggregateIfAlreadyRunning(boolean shouldAggregateIfAlreadyRunning) {
		this.shouldAggregateIfAlreadyRunning = shouldAggregateIfAlreadyRunning;
		return this;
	}
	
	public ETCDDataProcessorBuilder client(Client client) {
		this.client = client;
		return this;
	}

	public ETCDDataProcessor build() {
		return new ETCDDataProcessor(this.numOfThreads, this.retryDelay, this.retryDelayTimeUnit, this.retries,
			this.dataObjectProcessor, this.dataObjectProcessResultHandler, this.failureHandler,
			this.shouldAggregateIfAlreadyRunning, this.etcdUrl, client, randomGenerator);
	}
}
