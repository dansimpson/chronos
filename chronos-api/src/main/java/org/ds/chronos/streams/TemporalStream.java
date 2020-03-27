package org.ds.chronos.streams;

import java.util.stream.Stream;

import org.ds.chronos.api.Temporal;

public class TemporalStream<T> implements Temporal {

	private final long timestamp;
	private final Stream<T> stream;

	public TemporalStream(long timestamp, Stream<T> stream) {
		super();
		this.timestamp = timestamp;
		this.stream = stream;
	}

	/**
	 * @return the timestamp
	 */
	public long getTimestamp() {
		return timestamp;
	}

	/**
	 * @return the stream
	 */
	public Stream<T> getStream() {
		return stream;
	}

}
