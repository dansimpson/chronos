package org.ds.chronos.streams;

import java.util.Iterator;

import org.ds.chronos.api.Temporal;

public abstract class TemporalIterator<T extends Temporal> implements Iterator<T>, Temporal {

	private final long timestamp;

	public TemporalIterator(long timestamp) {
		this.timestamp = timestamp;
	}

	public long getTimestamp() {
		return timestamp;
	}

}
