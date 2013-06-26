package org.ds.chronos.timeline.msgpack;

import java.util.Iterator;

import org.ds.chronos.api.ChronologicalRecord;
import org.ds.chronos.api.TimelineEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encoder of objects to JSON for a timeline.
 * 
 * @author Dan Simpson
 * 
 * @param <T>
 */
public class MsgPackTimelineEncoder<T extends Timestamped> implements TimelineEncoder<T> {

	private static final Logger log = LoggerFactory.getLogger(MsgPackTimelineEncoder.class);

	private Iterator<T> upstream;

	public MsgPackTimelineEncoder() {
	}

	@Override
	public boolean hasNext() {
		return upstream.hasNext();
	}

	@Override
	public ChronologicalRecord next() {
		T item = upstream.next();
		try {
			return new ChronologicalRecord(item.getTimestamp(), MsgPackTimeline.msgpack.write(item));
		} catch (Throwable t) {
			log.error("Error encoding object", t);
		}
		return null;
	}

	@Override
	public void remove() {
	}

	@Override
	public void setInputStream(Iterator<T> input) {
		upstream = input;
	}

}
