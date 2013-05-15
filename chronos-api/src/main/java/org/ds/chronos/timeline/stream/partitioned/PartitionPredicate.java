package org.ds.chronos.timeline.stream.partitioned;

import com.google.common.base.Predicate;

/**
 * A predicate designed specifically for determining if a DataStream object belongs in a given partition
 * 
 * @author Dan Simpson
 * 
 * @param <T>
 */
public interface PartitionPredicate<T> extends Predicate<T> {

	/**
	 * When the predicate fails, the caller will want to advance the predicate for the rest of the sequence
	 */
	public void advance();
}
