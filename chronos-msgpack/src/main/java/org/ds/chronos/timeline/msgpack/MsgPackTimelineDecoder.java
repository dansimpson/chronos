package org.ds.chronos.timeline.msgpack;

import java.util.Iterator;

import org.ds.chronos.api.ChronologicalRecord;
import org.ds.chronos.api.TimelineDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Timeline decoder which converts stored JSON into objects of generic type.
 * 
 * @author Dan Simpson
 * 
 * @param <T>
 */
public class MsgPackTimelineDecoder<T extends Timestamped> implements TimelineDecoder<T> {

	private static final Logger log = LoggerFactory.getLogger(MsgPackTimelineEncoder.class);

	private Class<T> klass;

	public MsgPackTimelineDecoder(Class<T> klass) {
		this.klass = klass;
	}

	private Iterator<ChronologicalRecord> upstream;

	@Override
	public boolean hasNext() {
		return upstream.hasNext();
	}

	@Override
	public T next() {
		ChronologicalRecord column = upstream.next();
		T item = null;
		try {
			item = MsgPackTimeline.msgpack.read(column.getData(), klass);
			item.setTimestamp(column.getTimestamp());
		} catch (Throwable t) {
			log.error("Error decoding column", t);
		}
		return item;
	}

	@Override
	public void remove() {
	}

	@Override
	public void setInputStream(Iterator<ChronologicalRecord> input) {
		upstream = input;
	}

}
