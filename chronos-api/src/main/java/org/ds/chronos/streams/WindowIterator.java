package org.ds.chronos.streams;

import java.time.Duration;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.ds.chronos.api.Temporal;

/**
 * Window
 * 
 * @author dan
 *
 * @param <T>
 */
public class WindowIterator<T extends Temporal> implements Iterator<TemporalIterator<T>> {

	private long threshold = Long.MIN_VALUE;
	private T next;

	private final Iterator<T> source;
	private final long window;

	public WindowIterator(Iterator<T> source, Duration window) {
		this(source, window.toMillis());
	}

	public WindowIterator(Iterator<T> source, long window) {
		super();
		this.source = source;
		this.window = window;
	}

	@Override
	public boolean hasNext() {
		return source.hasNext();
	}

	private void advance() {
		if (hasNext()) {
			next = source.next();
			if (next.getTimestamp() >= threshold) {
				if (threshold == Long.MIN_VALUE) {
					threshold = next.getTimestamp() + window;
				} else {
					threshold += window;
				}
			}
		} else {
			threshold = Long.MIN_VALUE;
		}
	}

	private T getAndAdvance() {
		T item = next;
		advance();
		return item;
	}

	@Override
	public TemporalIterator<T> next() {
		// Advance for the first check
		if (next == null) {
			advance();
		}

		long track = threshold;

		// Handle a data gap by advancing and returning an empty iterator
		if (next.getTimestamp() >= threshold) {
			threshold += window;
		}

		return new TemporalIterator<T>(track - window) {

			@Override
			public boolean hasNext() {
				return next.getTimestamp() < track && next.getTimestamp() < threshold;
			}

			@Override
			public T next() {
				return getAndAdvance();
			}

		};
	}

	private <K> Stream<K> stream(Iterator<K> iterator) {
		return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
	}

	/**
	 * @return Stream wrapping the iterators
	 */
	public Stream<TemporalStream<T>> stream() {
		return stream(this).map(i -> new TemporalStream<T>(i.getTimestamp(), stream(i)));
	}

}