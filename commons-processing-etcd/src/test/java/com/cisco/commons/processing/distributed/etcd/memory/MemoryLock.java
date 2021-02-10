package com.cisco.commons.processing.distributed.etcd.memory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import com.cisco.commons.processing.distributed.etcd.ETCDDataProcessor;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Lock;
import io.etcd.jetcd.lock.LockResponse;
import io.etcd.jetcd.lock.UnlockResponse;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MemoryLock implements Lock {
	
	private Map<String, java.util.concurrent.locks.Lock> locks = new ConcurrentHashMap<>();

	@Override
	public CompletableFuture<LockResponse> lock(ByteSequence name, long leaseId) {
		String lockKeyStr = name.toString(ETCDDataProcessor.DEFAULT_CHARSET);
		locks.putIfAbsent(lockKeyStr, new ReentrantLock());
		java.util.concurrent.locks.Lock lock = locks.get(lockKeyStr);
		log.info("Locking: {}", lockKeyStr);
		lock.lock();
		io.etcd.jetcd.api.lock.LockResponse response = io.etcd.jetcd.api.lock.LockResponse.newBuilder().build();
		LockResponse lockResponse = new LockResponse(response , MemoryClient.NAMESPACE);
		CompletableFuture<LockResponse> future = new CompletableFuture<>();
		future.complete(lockResponse);
		return future;
	}

	@Override
	public CompletableFuture<UnlockResponse> unlock(ByteSequence lockKey) {
		String lockKeyStr = lockKey.toString(ETCDDataProcessor.DEFAULT_CHARSET);
		log.info("Unlocking: {}", lockKeyStr);
		java.util.concurrent.locks.Lock lock = locks.get(lockKeyStr);
		if (lock != null) {
			lock.unlock();
		}
		io.etcd.jetcd.api.lock.UnlockResponse response = io.etcd.jetcd.api.lock.UnlockResponse.newBuilder().build();
		UnlockResponse unlockResponse = new UnlockResponse(response );
		CompletableFuture<UnlockResponse> future = new CompletableFuture<>();
		future.complete(unlockResponse);
		return future;
	}

}
