package com.cisco.commons.processing.distributed.etcd;

/**
 * Random string generator.
 * This is used for adding a random string to a timestamp to prevent situations of multiple entries are calculated
 * at the same time.
 * Default implementation has slow performance, and user can use a custom fast implementation, as the probability 
 * of colliions is very low.
 * @author Liran Mendelovich
 *
 */
public interface RandomGenerator {
	public String generateRandomString();
}
