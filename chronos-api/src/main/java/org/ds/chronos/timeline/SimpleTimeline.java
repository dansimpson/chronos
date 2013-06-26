package org.ds.chronos.timeline;

import java.util.Arrays;
import java.util.Iterator;

import org.ds.chronos.api.Chronicle;
import org.ds.chronos.api.Temporal;
import org.ds.chronos.api.Timeline;
import org.ds.chronos.api.TimelineDecoder;
import org.ds.chronos.api.TimelineEncoder;

/**
 * 
 * A high level timeseries store with streaming encoding and decoding.
 * <p>
 * Given a {@link Chronicle}, {@link TimelineEncoder}, and {@link TimelineDecoder}; we can produce a data layer for persisting and fetching
 * time stamped business objects.
 * 
 * @author Dan Simpson
 * 
 * @param <T>
 *          The type of object
 */
public class SimpleTimeline<T extends Temporal> extends Timeline<T> {

	protected Chronicle chronicle;

	protected TimelineDecoder<T> decoder;
	protected TimelineEncoder<T> encoder;

	/**
	 * 
	 * @param chronicle
	 * @param decoder
	 * @param encoder
	 */
	public SimpleTimeline(Chronicle chronicle, TimelineDecoder<T> decoder, TimelineEncoder<T> encoder) {
		this.chronicle = chronicle;
		this.decoder = decoder;
		this.encoder = encoder;
	}

	/**
	 * Add a batch of data
	 * 
	 * @param data
	 * @param batchSize
	 */
	public synchronized void add(Iterator<T> data, int batchSize) {
		encoder.setInputStream(data);
		chronicle.add(encoder, batchSize);
	}

	/**
	 * Add a single T to the chronicle
	 * 
	 * @param item
	 *          T object
	 */
	@SuppressWarnings("unchecked")
	public void add(T item) {
		add(Arrays.asList(item).iterator(), 1);
	}

	/**
	 * Fetch a stream of metrics for a given timeframe
	 * 
	 * @param d1
	 * @param d2
	 * @return iterable stream of metrics
	 */
	public synchronized Iterator<T> buildIterator(long t1, long t2, int batchSize) {
		decoder.setInputStream(chronicle.getRange(t1, t2, batchSize));
		return decoder;
	}

	/**
	 * @param t1
	 * @param t2
	 * @return
	 * @see org.ds.chronos.api.Chronicle#getNumEvents(long, long)
	 */
	public long getNumEvents(long t1, long t2) {
		return chronicle.getNumEvents(t1, t2);
	}

	/**
	 * @param t1
	 * @return
	 * @see org.ds.chronos.api.Chronicle#isEventRecorded(long)
	 */
	public boolean isEventRecorded(long t1) {
		return chronicle.isEventRecorded(t1);
	}

	/**
	 * @param t1
	 * @param t2
	 * @see org.ds.chronos.api.Chronicle#deleteRange(long, long)
	 */
	public void deleteRange(long t1, long t2) {
		chronicle.deleteRange(t1, t2);
	}

}
