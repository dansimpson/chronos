package org.ds.chronos.timeline.stream.partitioned;

/**
 * A parition predicate which puts items in buckets based on count
 * 
 * @author Dan Simpson
 * 
 * @param <T>
 */
public class BucketSizePredicate<T> implements PartitionPredicate<T> {

	private int count = 0;
	private final int bucketSize;

	public BucketSizePredicate(int bucketSize) {
		this.bucketSize = bucketSize;
	}

	public boolean apply(T t) {
		return count++ < bucketSize;
	}

	public void advance() {
		count = 0;
	}

}
