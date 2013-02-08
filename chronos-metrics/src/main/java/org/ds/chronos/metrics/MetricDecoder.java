package org.ds.chronos.metrics;

import java.util.Iterator;

import org.ds.chronos.api.ChronologicalRecord;
import org.ds.chronos.timeline.TimelineDecoder;

/**
 * Streaming decoder for metrics stored on a timeline.
 * 
 * @author Dan Simpson
 * 
 */
public class MetricDecoder implements TimelineDecoder<Metric> {

  private Iterator<ChronologicalRecord> upstream;

  @Override
  public boolean hasNext() {
    return upstream.hasNext();
  }

  @Override
  public Metric next() {
    ChronologicalRecord item = upstream.next();
    return new Metric(item.getTimestamp(), item.getValueBytes().getFloat());
  }

  @Override
  public void remove() {
  }

  @Override
  public void setInputStream(Iterator<ChronologicalRecord> input) {
    upstream = input;
  }

}
