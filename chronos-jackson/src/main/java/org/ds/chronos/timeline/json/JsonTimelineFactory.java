package org.ds.chronos.timeline.json;

import org.codehaus.jackson.type.TypeReference;
import org.ds.chronos.api.Chronos;
import org.ds.chronos.api.ChronosException;
import org.ds.chronos.chronicle.PartitionPeriod;
import org.ds.chronos.timeline.Timeline;

/**
 * Factory for creating JSON Timelines
 * 
 * @see Timeline
 * @author Dan Simpson
 * 
 */
public class JsonTimelineFactory {

  private Chronos chronos;

  public JsonTimelineFactory(Chronos chronos) {
    this.chronos = chronos;
  }

  /**
   * Create a Timeline for storing json objects
   * 
   * @param key
   * @return
   * @throws ChronosException
   */
  public <T extends Timestamped> Timeline<T> createTimeline(String key,
      TypeReference<T> ref) throws ChronosException {
    return chronos.getTimeline(key, new JsonTimelineEncoder<T>(),
        new JsonTimelineDecoder<T>(ref));
  }

  /**
   * Create a Timeline for storing json object with paritioning
   * 
   * @param key
   * @param period
   * @return
   * @throws ChronosException
   */
  public <T extends Timestamped> Timeline<T> createTimeline(String key,
      PartitionPeriod period, TypeReference<T> ref) throws ChronosException {
    return chronos.getTimeline(key, new JsonTimelineEncoder<T>(),
        new JsonTimelineDecoder<T>(ref), period);
  }
}
