package org.ds.chronos.timeline.json;

import java.util.Iterator;

import org.ds.chronos.api.ChronologicalRecord;
import org.ds.chronos.api.TimelineEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Encoder of objects to JSON for a timeline.
 * 
 * @author Dan Simpson
 * 
 * @param <T>
 */
public class JsonTimelineEncoder<T extends Timestamped> implements TimelineEncoder<T> {

	private static final Logger log = LoggerFactory.getLogger(JsonTimelineEncoder.class);

	private static final ObjectMapper mapper = new ObjectMapper();
	static {
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	private Iterator<T> upstream;

	public JsonTimelineEncoder() {
	}

	@Override
	public boolean hasNext() {
		return upstream.hasNext();
	}

	@Override
	public ChronologicalRecord next() {
		T item = upstream.next();
		try {
			return new ChronologicalRecord(item.getTimestamp(), mapper.writeValueAsBytes(item));
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
