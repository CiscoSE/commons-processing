package com.cisco.commons.processing.distributed.etcd;

import java.util.UUID;

/**
 * Random string generator based on slow performance random UUID.
 * This is used for adding a random string to a timestamp to prevent situations of multiple enries are calculated
 * at the same time.
 * @author Liran Mendelovich
 *
 */
public class UUIDRandomGenerator implements RandomGenerator {

	@Override
	public String generateRandomString() {
		return UUID.randomUUID().toString();
	}
}
