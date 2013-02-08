package org.ds.chronos.metrics;

import java.util.Iterator;

import org.ds.chronos.api.ChronologicalRecord;
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
  public ChronologicalRecord next() {
    Metric m = upstream.next();
    return new ChronologicalRecord(m.getTime(), m.toBuffer().array());
  }

  @Override
  public void remove() {
  }

  @Override
  public void setInputStream(Iterator<Metric> input) {
    upstream = input;
  }

}
