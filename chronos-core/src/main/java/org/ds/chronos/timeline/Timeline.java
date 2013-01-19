package org.ds.chronos.timeline;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

import org.ds.chronos.api.Chronicle;
import org.ds.chronos.timeline.stream.DataStream;

/**
 * 
 * A higher level timeseries store with streaming encoding and decoding.
 * <p>
 * Given a {@link Chronicle}, {@link TimelineEncoder}, and {@link TimelineDecoder};
 * we can produce a data layer for persisting and fetching
 * time stamped business objects.
 * 
 * @author Dan
 *
 * @param <T> The type of object
 */
public class Timeline<T> {

	protected Chronicle chronicle;
	
	protected TimelineDecoder<T> decoder;
	protected TimelineEncoder<T> encoder;
	
	/**
	 * 
	 * @param chronicle
	 * @param decoder
	 * @param encoder
	 */
	public Timeline(Chronicle chronicle, TimelineDecoder<T> decoder, TimelineEncoder<T> encoder) {
		this.chronicle = chronicle;
		this.decoder = decoder;
		this.encoder = encoder;
	}

	/**
	 * Add a batch of data
	 * @param data
	 * @param batchSize
	 */
	public synchronized void add(Iterator<T> data, int batchSize) {
		encoder.setInputStream(data);
		chronicle.add(encoder, batchSize);
	}
	
	/**
	 * @see #add(Iterator, int)
	 * @param data
	 */
	public void add(Collection<T> data) {
		add(data.iterator(), Chronicle.WRITE_PAGE_SIZE);
	}
	
	/**
	 * Add a single T to the chronicle
	 * @param item T object
	 */
	@SuppressWarnings("unchecked")
	public void add(T item) {
		add(Arrays.asList(item).iterator(), 1);
	}
	
	/**
	 * Fetch a stream of metrics for a given timeframe
	 * @param d1
	 * @param d2
	 * @return iterable stream of metrics
	 */
	public synchronized Iterator<T> getRange(long t1, long t2, int batchSize) {
		decoder.setInputStream(chronicle.getRange(t1, t2, batchSize));
		return decoder;
	}
	
	/**
	 * Fetch a lazy data stream for the time range, of which has
	 * stages that ultimately produce objects of the output class
	 * @param t1 from time
	 * @param t2 to time
	 * @param klass the type of the output (post transformation)
	 * @return
	 */
	public <K> DataStream<K> query(long t1, long t2, Class<K> klass) {
		return new DataStream<K>(getRange(t1, t2, Chronicle.READ_PAGE_SIZE));
	}
	
	/**
	 * @see #query(long, long, Class)
	 * @param t1
	 * @param t2
	 * @param klass
	 * @return
	 */
	public <K> DataStream<K> query(Date t1, Date t2, Class<K> klass) {
		return query(t1.getTime(), t2.getTime(), klass);
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
	 * @param d1
	 * @param d2
	 * @return
	 * @see org.ds.chronos.api.Chronicle#getNumEvents(java.util.Date, java.util.Date)
	 */
	public long getNumEvents(Date d1, Date d2) {
		return chronicle.getNumEvents(d1, d2);
	}

	/**
	 * @param t1
	 * @param t2
	 * @see org.ds.chronos.api.Chronicle#deleteRange(long, long)
	 */
	public void deleteRange(long t1, long t2) {
		chronicle.deleteRange(t1, t2);
	}

	/**
	 * @param t1
	 * @param t2
	 * @see org.ds.chronos.api.Chronicle#deleteRange(java.util.Date, java.util.Date)
	 */
	public void deleteRange(Date t1, Date t2) {
		chronicle.deleteRange(t1, t2);
	}
	
	
}
