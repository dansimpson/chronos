package org.ds.chronos.timeline.json;

import java.util.Iterator;

import me.prettyprint.hector.api.beans.HColumn;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.ds.chronos.timeline.TimelineDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonTimelineDecoder<T extends Timestamped> implements TimelineDecoder<T> {

	private static final Logger log = LoggerFactory.getLogger(JsonTimelineEncoder.class);
	private static final ObjectMapper mapper = new ObjectMapper();
	static {
		mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}
	
	private TypeReference<T> typeRef;
	
	public JsonTimelineDecoder(TypeReference<T> typeRef) {
		this.typeRef = typeRef;
	}
	
	private Iterator<HColumn<Long, byte[]>> upstream;
	
	@Override
	public boolean hasNext() {
		return upstream.hasNext();
	}

	@Override
	public T next() {
		HColumn<Long, byte[]> column = upstream.next();
		T item = null;
		try {
			item = mapper.readValue(column.getValue(), typeRef);
			item.setTimestamp(column.getName());
		} catch (Throwable t) {
			log.error("Error decoding column", t);
		}
		return item;
	}

	@Override
	public void remove() {
	}

	@Override
	public void setInputStream(Iterator<HColumn<Long, byte[]>> input) {
		upstream = input;
	}

}
