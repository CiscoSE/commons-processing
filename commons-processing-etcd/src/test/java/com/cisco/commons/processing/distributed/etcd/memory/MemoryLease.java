package com.cisco.commons.processing.distributed.etcd.memory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.etcd.jetcd.Lease;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.lease.LeaseRevokeResponse;
import io.etcd.jetcd.lease.LeaseTimeToLiveResponse;
import io.etcd.jetcd.options.LeaseOption;
import io.etcd.jetcd.support.CloseableClient;
import io.grpc.stub.StreamObserver;

public class MemoryLease implements Lease {

	@Override
	public CompletableFuture<LeaseGrantResponse> grant(long ttl) {
		io.etcd.jetcd.api.LeaseGrantResponse response = io.etcd.jetcd.api.LeaseGrantResponse.newBuilder().build();
		LeaseGrantResponse leaseGrantResponse = new LeaseGrantResponse(response);
		CompletableFuture<LeaseGrantResponse> future = new CompletableFuture<>();
		future.complete(leaseGrantResponse);
		return future;
	}

	@Override
	public CompletableFuture<LeaseGrantResponse> grant(long ttl, long timeout, TimeUnit unit) {
		return null;
	}

	@Override
	public CompletableFuture<LeaseRevokeResponse> revoke(long leaseId) {
		return null;
	}

	@Override
	public CompletableFuture<LeaseKeepAliveResponse> keepAliveOnce(long leaseId) {
		return null;
	}

	@Override
	public CompletableFuture<LeaseTimeToLiveResponse> timeToLive(long leaseId, LeaseOption leaseOption) {
		return null;
	}

	@Override
	public CloseableClient keepAlive(long leaseId, StreamObserver<LeaseKeepAliveResponse> observer) {
		return null;
	}

}
