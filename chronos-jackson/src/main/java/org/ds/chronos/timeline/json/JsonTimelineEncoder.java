package org.ds.chronos.timeline.json;

import java.util.Iterator;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.ds.chronos.api.ChronologicalRecord;
import org.ds.chronos.timeline.TimelineEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encoder of objects to JSON for a timeline.
 * 
 * @author Dan Simpson
 *
 * @param <T>
 */
public class JsonTimelineEncoder<T extends Timestamped> implements
    TimelineEncoder<T> {

  private static final Logger log = LoggerFactory
      .getLogger(JsonTimelineEncoder.class);

  private static final ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES,
        false);
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
      return new ChronologicalRecord(item.getTimestamp(),
          mapper.writeValueAsBytes(item));
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
