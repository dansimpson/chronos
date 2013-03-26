package org.ds.chronos.timeline.stream.partitioned;

import org.ds.chronos.api.Temporal;
import org.ds.chronos.util.Duration;

/**
 * A partition predicate which leverage timestamps on temporal objects to lump them into time frame buckets
 * 
 * @author Dan Simpson
 * 
 * @param <T>
 */
public class TimeRangePredicate<T extends Temporal> implements PartitionPredicate<T> {

	private Long start;
	private final long duration;
	private T last;

	/**
	 * The size of the window, as a duration string see {@link Duration}
	 * 
	 * @param duration
	 */
	public TimeRangePredicate(String duration) {
		this(new Duration(duration));
	}

	/**
	 * The size of the window, as a {@link Duration}
	 * 
	 * @param duration
	 */
	public TimeRangePredicate(Duration duration) {
		this.duration = duration.getMillis();
	}

	public boolean apply(T object) {
		last = object;
		if (start == null) {
			start = last.getTimestamp();
		}

		return last.getTimestamp() >= start && last.getTimestamp() <= (start + duration);
	}

	public void advance() {
		start = last.getTimestamp();
	}

}
