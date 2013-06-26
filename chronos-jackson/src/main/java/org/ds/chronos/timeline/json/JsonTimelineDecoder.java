package org.ds.chronos.timeline.json;

import java.util.Iterator;

import org.ds.chronos.api.ChronologicalRecord;
import org.ds.chronos.api.TimelineDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A Timeline decoder which converts stored JSON into objects of generic type.
 * 
 * @author Dan Simpson
 * 
 * @param <T>
 */
public class JsonTimelineDecoder<T extends Timestamped> implements TimelineDecoder<T> {

	private static final Logger log = LoggerFactory.getLogger(JsonTimelineEncoder.class);
	private static final ObjectMapper mapper = new ObjectMapper();
	static {
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	private TypeReference<T> typeRef;

	public JsonTimelineDecoder(TypeReference<T> typeRef) {
		this.typeRef = typeRef;
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
			item = mapper.readValue(column.getData(), typeRef);
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
