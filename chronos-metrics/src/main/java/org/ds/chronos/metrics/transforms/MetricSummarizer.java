package org.ds.chronos.metrics.transforms;

import org.ds.chronos.metrics.Metric;
import org.ds.chronos.metrics.MetricSummary;
import org.ds.chronos.timeline.stream.DataStreamAggregator.Aggregator;
import org.ds.chronos.util.Duration;

/**
 * Duration based aggregator for a stream of metrics.
 * 
 * @author Dan Simpson
 * 
 */
public class MetricSummarizer implements Aggregator<Metric, MetricSummary> {

  private MetricSummary aggregate = new MetricSummary();
  private Duration duration;

  public MetricSummarizer(String duration) {
    this.duration = new Duration(duration);
  }

  public MetricSummarizer(Duration duration) {
    this.duration = duration;
  }

  @Override
  public boolean feed(Metric metric) {
    aggregate.add(metric);
    return aggregate.getDuration() > duration.getMillis();
  }

  @Override
  public MetricSummary flush() {
    return aggregate.cloneAndReset();
  }

}
