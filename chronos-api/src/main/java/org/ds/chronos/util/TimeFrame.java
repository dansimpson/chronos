package org.ds.chronos.util;

import java.util.Date;

/**
 * Representation of a time frame.
 * 
 * @author Dan Simpson
 * 
 */
public class TimeFrame {

	private final long t1;
	private final long t2;

	public TimeFrame(long start, long end) {
		this.t1 = start;
		this.t2 = end;
	}

	public TimeFrame(long start, Duration duration) {
		this(start, start + duration.getMillis());
	}

	public TimeFrame(Date start, Date end) {
		this(start.getTime(), end.getTime());
	}

	public Duration getDuration() {
		return new Duration(t2 - t1);
	}

	/**
	 * @return the t1
	 */
	public long getStart() {
		return t1;
	}

	/**
	 * @return the t2
	 */
	public long getEnd() {
		return t2;
	}

	public boolean contains(long time) {
		return time >= t1 && time <= t2;
	}

	/**
	 * Check if an entire time frame is contained in this time frame
	 * 
	 * @param start
	 * @param end
	 * @return
	 */
	public boolean contains(long start, long end) {
		return contains(start) && contains(end);
	}

	/**
	 * Check if this time frame intersects another time frame
	 * 
	 * @param start
	 * @param end
	 * @return
	 */
	public boolean intersects(long start, long end) {
		return contains(start) || contains(end) || (t1 > start && t2 < end);
	}

	/**
	 * Fit this time frame within another time frame, or return null if impossible.
	 * 
	 * @param start
	 * @param end
	 * @return
	 */
	public TimeFrame fit(long start, long end) {
		if (!intersects(start, end)) {
			return null;
		}
		return new TimeFrame(Math.max(start, t1), Math.min(end, t2));
	}

}
