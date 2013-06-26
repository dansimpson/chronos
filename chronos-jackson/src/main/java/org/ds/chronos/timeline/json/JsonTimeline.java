package org.ds.chronos.timeline.json;

import org.ds.chronos.api.Chronicle;
import org.ds.chronos.timeline.SimpleTimeline;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * JSON Timeline
 * 
 * @author Dan Simpson
 *
 * @param <T> type which can be serialized to and from json
 */
public class JsonTimeline<T extends Timestamped> extends SimpleTimeline<T> {

  /**
   * Create a new JSON Timeline
   * @param chronicle the underlying storage
   * @param ref the typeref for decoding
   */
  public JsonTimeline(Chronicle chronicle, TypeReference<T> ref) {
    super(chronicle, new JsonTimelineDecoder<T>(ref), new JsonTimelineEncoder<T>());
  }

}
