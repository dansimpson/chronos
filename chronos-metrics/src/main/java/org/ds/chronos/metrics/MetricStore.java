package org.ds.chronos.metrics;

import org.ds.chronos.api.Chronicle;
import org.ds.chronos.timeline.Timeline;

/**
 * A Timeline for storing metrics.
 * 
 * @author Dan Simpson
 * 
 */
public class MetricStore extends Timeline<Metric> {

  public MetricStore(Chronicle chronicle) {
    super(chronicle, new MetricDecoder(), new MetricEncoder());
  }

}
