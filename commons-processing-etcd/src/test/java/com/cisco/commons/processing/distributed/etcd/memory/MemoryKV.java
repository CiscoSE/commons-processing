package com.cisco.commons.processing.distributed.etcd.memory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.ByteString;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Txn;
import io.etcd.jetcd.api.DeleteRangeResponse;
import io.etcd.jetcd.api.KeyValue;
import io.etcd.jetcd.api.PutResponse.Builder;
import io.etcd.jetcd.api.RangeResponse;
import io.etcd.jetcd.kv.CompactResponse;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.options.CompactOption;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;

public class MemoryKV implements KV {

	private static final Charset CHARSET = StandardCharsets.UTF_8;
	private Map<ByteSequence, ByteSequence> map = new ConcurrentHashMap<>();
	
	@Override
	public CompletableFuture<PutResponse> put(ByteSequence key, ByteSequence value) {
		Builder apiPutResponseBuilder = io.etcd.jetcd.api.PutResponse.newBuilder();
		ByteSequence prev = map.put(key, value);
		if (prev != null) {
			KeyValue prevKv = buildKV(key, value);
			apiPutResponseBuilder.setPrevKv(prevKv);
		}
		io.etcd.jetcd.api.PutResponse apiPutResponse = apiPutResponseBuilder.build();
		PutResponse putResponse = new PutResponse(apiPutResponse, MemoryClient.NAMESPACE);
		CompletableFuture<PutResponse> future = new CompletableFuture<>();
		future.complete(putResponse);
		return future;
	}

	private KeyValue buildKV(ByteSequence key, ByteSequence value) {
		ByteString keyByteString = ByteString.copyFrom(key.getBytes());
		ByteString valueByteString = ByteString.copyFrom(value.getBytes());
		KeyValue prevKv = KeyValue.newBuilder().setKey(keyByteString).setValue(valueByteString).build();
		return prevKv;
	}

	@Override
	public CompletableFuture<PutResponse> put(ByteSequence key, ByteSequence value, PutOption option) {
		return null;
	}

	@Override
	public CompletableFuture<GetResponse> get(ByteSequence key) {
		return null;
	}

	// TODO highly inefficient
	@Override
	public CompletableFuture<GetResponse> get(ByteSequence key, GetOption option) {
		long limit = option.getLimit();
		Map<String, ByteSequence> tempMap = new TreeMap<>();
		Set<Entry<ByteSequence, ByteSequence>> entries = map.entrySet();
		for (Entry<ByteSequence, ByteSequence> entry : entries) {
			tempMap.put(entry.getKey().toString(CHARSET), entry.getValue());
		}
		Set<Entry<String, ByteSequence>> tempMapEntries = tempMap.entrySet();
		io.etcd.jetcd.api.RangeResponse.Builder rangeResponseBuilder = RangeResponse.newBuilder();
		
		int count = 0;
//		for (Entry<String, ByteSequence> entry : tempMapEntries) {
		Iterator<Entry<String, ByteSequence>> it = tempMapEntries.iterator();
		while (count < limit && it.hasNext()) {
			Entry<String, ByteSequence> entry = it.next();
			String keyStr = key.toString(CHARSET);
			if (entry.getKey().startsWith(keyStr)) {
				ByteSequence keyByteSeq = ByteSequence.from(entry.getKey(), CHARSET);
				KeyValue kv = buildKV(keyByteSeq , entry.getValue());
				rangeResponseBuilder.addKvs(kv);
				count++;
			}
		}
		GetResponse getResponse = new GetResponse(rangeResponseBuilder.build() , MemoryClient.NAMESPACE);
		CompletableFuture<GetResponse> future = new CompletableFuture<>();
		future.complete(getResponse);
		return future;
	}

	@Override
	public CompletableFuture<DeleteResponse> delete(ByteSequence key) {
		map.remove(key);
		DeleteRangeResponse deleteRangeResponse = DeleteRangeResponse.newBuilder().setDeleted(1).build();
		DeleteResponse deleteResponse = new DeleteResponse(deleteRangeResponse, MemoryClient.NAMESPACE);
		CompletableFuture<DeleteResponse> future = new CompletableFuture<>();
		future.complete(deleteResponse);
		return future;
	}

	@Override
	public CompletableFuture<DeleteResponse> delete(ByteSequence key, DeleteOption option) {
		return null;
	}

	@Override
	public CompletableFuture<CompactResponse> compact(long rev) {
		return null;
	}

	@Override
	public CompletableFuture<CompactResponse> compact(long rev, CompactOption option) {
		return null;
	}

	@Override
	public Txn txn() {
		return null;
	}

}
