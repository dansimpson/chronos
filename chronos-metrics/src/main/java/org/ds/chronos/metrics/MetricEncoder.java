package org.ds.chronos.metrics;

import java.util.Iterator;

import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;

import org.ds.chronos.timeline.TimelineEncoder;

/**
 * Streaming encoder for metrics to be stored on a timeline
 * 
 * @author Dan Simpson
 * 
 */
public class MetricEncoder implements TimelineEncoder<Metric> {

  private Iterator<Metric> upstream;

  @Override
  public boolean hasNext() {
    return upstream.hasNext();
  }

  @Override
  public HColumn<Long, byte[]> next() {
    Metric m = upstream.next();
    return HFactory.createColumn(m.getTime(), m.toBuffer().array());
  }

  @Override
  public void remove() {
  }

  @Override
  public void setInputStream(Iterator<Metric> input) {
    upstream = input;
  }

}
