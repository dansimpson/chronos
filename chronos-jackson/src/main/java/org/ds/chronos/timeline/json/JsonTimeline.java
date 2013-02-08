package org.ds.chronos.timeline.json;

import org.codehaus.jackson.type.TypeReference;
import org.ds.chronos.api.Chronicle;
import org.ds.chronos.timeline.Timeline;

/**
 * JSON Timeline
 * 
 * @author Dan Simpson
 *
 * @param <T> type which can be serialized to and from json
 */
public class JsonTimeline<T extends Timestamped> extends Timeline<T> {

  /**
   * Create a new JSON Timeline
   * @param chronicle the underlying storage
   * @param ref the typeref for decoding
   */
  public JsonTimeline(Chronicle chronicle, TypeReference<T> ref) {
    super(chronicle, new JsonTimelineDecoder<T>(ref), new JsonTimelineEncoder<T>());
  }

}
