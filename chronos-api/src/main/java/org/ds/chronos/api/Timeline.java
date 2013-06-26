package org.ds.chronos.api;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

import org.ds.chronos.timeline.stream.DataStream;

public abstract class Timeline<T extends Temporal> {

	public static final int DEFAULT_BATCH_WRITE_SIZE = 512;
	public static final int DEFAULT_BATCH_READ_SIZE = 2048;

	protected int batchReadSize = DEFAULT_BATCH_READ_SIZE;
	protected int batchWriteSize = DEFAULT_BATCH_WRITE_SIZE;

	/**
	 * Iterate over items and persist in batches of batchSize
	 * 
	 * @param data
	 * @param batchSize
	 */
	public abstract void add(Iterator<T> data, int batchSize);

	/**
	 * Add a collection of T items to the timeline
	 * 
	 * @param data
	 */
	public void add(Collection<T> data) {
		add(data.iterator(), batchWriteSize);
	}

	/**
	 * Add a single T to the chronicle
	 * 
	 * @param item
	 *          T object
	 */
	public abstract void add(T item);

	/**
	 * Fetch a stream of metrics for a given timeframe
	 * 
	 * @param d1
	 * @param d2
	 * @return iterable stream of metrics
	 */
	public abstract Iterator<T> buildIterator(long t1, long t2, int batchSize);

	public Iterator<T> buildIterator(long t1, long t2) {
		return buildIterator(t1, t2, batchReadSize);
	}

	/**
	 * Iterable of timeline for a given range
	 * 
	 * @param t1
	 * @param t2
	 * @return
	 */
	public Iterable<T> iterable(final long t1, final long t2) {
		return new Iterable<T>() {

			@Override
			public Iterator<T> iterator() {
				return buildIterator(t1, t2);
			}

		};
	}

	/**
	 * Fetch a lazy data stream for the time range, of which has stages that ultimately produce objects of the output class
	 * 
	 * @param t1
	 *          from time
	 * @param t2
	 *          to time
	 * @param klass
	 *          the type of the output (post transformation)
	 * @return
	 */
	public DataStream<T> query(long t1, long t2) {
		return new DataStream<T>(iterable(t1, t2));
	}

	/**
	 * Fetch a lazy data stream for the time range, of which has stages that ultimately produce objects of the output class
	 * 
	 * @param t1
	 * @param t2
	 * @return
	 */
	public DataStream<T> query(Date t1, Date t2) {
		return query(t1.getTime(), t2.getTime());
	}

	/**
	 * @param t1
	 * @param t2
	 * @return
	 * @see org.ds.chronos.api.Chronicle#getNumEvents(long, long)
	 */
	public abstract long getNumEvents(long t1, long t2);

	public long getNumEvents(Date d1, Date d2) {
		return getNumEvents(d1.getTime(), d2.getTime());
	}

	/**
	 * @param t1
	 * @return
	 * @see org.ds.chronos.api.Chronicle#isEventRecorded(long)
	 */
	public abstract boolean isEventRecorded(long time);

	public boolean isEventRecorded(Date date) {
		return isEventRecorded(date.getTime());
	}

	/**
	 * @param t1
	 * @param t2
	 * @see org.ds.chronos.api.Chronicle#deleteRange(long, long)
	 */
	public abstract void deleteRange(long t1, long t2);

	public void deleteRange(Date t1, Date t2) {
		deleteRange(t1.getTime(), t2.getTime());
	}

}
