package org.ds.chronos.metrics;

import java.util.Iterator;

import me.prettyprint.hector.api.beans.HColumn;

import org.ds.chronos.timeline.TimelineDecoder;

/**
 * Streaming decoder for metrics stored on a timeline.
 * 
 * @author Dan Simpson
 * 
 */
public class MetricDecoder implements TimelineDecoder<Metric> {

  private Iterator<HColumn<Long, byte[]>> upstream;

  @Override
  public boolean hasNext() {
    return upstream.hasNext();
  }

  @Override
  public Metric next() {
    HColumn<Long, byte[]> item = upstream.next();
    return new Metric(item.getName(), item.getValueBytes().getFloat());
  }

  @Override
  public void remove() {
  }

  @Override
  public void setInputStream(Iterator<HColumn<Long, byte[]>> input) {
    upstream = input;
  }

}
