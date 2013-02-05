package org.ds.chronos.metrics.archive;

import java.util.Iterator;

import org.ds.chronos.api.Chronicle;
import org.ds.chronos.metrics.Metric;
import org.ds.chronos.timeline.Timeline;
import org.ds.chronos.util.Duration;

public class MetricArchive extends Timeline<Metric> {

  protected Duration window;

  /**
   * Create a metric archive, which compacts metrics for increased storage
   * efficiency and performance.
   * 
   * @param chronicle
   *          the chronicle where we store the metrcs
   * @param interval
   *          the known interval between each metric
   * @param window
   *          the time window for grouping metrics
   * 
   * @example If interval is "1m" and the window is "1h", the column will hold
   *          60 metrics, and be (60 * 4) + 8 bytes in size. The extra 8 bytes
   *          are for holding the interval.
   */
  public MetricArchive(Chronicle chronicle, Duration interval, Duration window) {
    super(chronicle, new MetricArchiveDecoder(), new MetricArchiveEncoder(
        interval, window));
    this.window = window;
  }

  @Override
  public Iterator<Metric> getRange(long t1, long t2, int batchSize) {
    return super.getRange(window.justifyPast(t1), window.justifyPast(t2),
        batchSize);
  }

}
