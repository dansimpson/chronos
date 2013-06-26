package org.ds.chronos.timeline;

import java.util.Iterator;

import org.ds.chronos.api.Temporal;
import org.ds.chronos.api.Timeline;


/**
 * Copy utility for Timelines
 * 
 * @author Dan Simpson
 * 
 * @param <T>
 */
public class TimelineCopy<T extends Temporal> {

	private final Timeline<T> source;
	private final Timeline<T> destination;

	public TimelineCopy(Timeline<T> source, Timeline<T> destination) {
		this.source = source;
		this.destination = destination;
	}

	/**
	 * Copy all events from source to destination
	 * 
	 * @param t1
	 * @param t2
	 * @param batchSize
	 */
	public void copy(long t1, long t2, int batchSize) {
		copy(t1, t2, batchSize, batchSize);
	}

	/**
	 * Copy all events from source to destination
	 * 
	 * @param t1
	 *          - start time
	 * @param t2
	 *          - end time
	 * @param readBatch
	 *          - number of items to read at a time
	 * @param writeBatch
	 *          - number of items to write at a time
	 */
	public void copy(long t1, long t2, int readBatch, int writeBatch) {
		Iterator<T> iterator = source.buildIterator(t1, t2, readBatch);
		destination.add(iterator, writeBatch);
	}

	/**
	 * Move items from source to destination, deleting source elements after write
	 * 
	 * @param t1
	 * @param t2
	 * @param readBatch
	 * @param writeBatch
	 */
	public void move(long t1, long t2, int readBatch, int writeBatch) {
		Iterator<T> iterator = source.buildIterator(t1, t2, readBatch);
		destination.add(iterator, writeBatch);
	}

}
