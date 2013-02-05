package org.ds.chronos.metrics.archive;

import java.nio.ByteBuffer;
import java.util.Iterator;

import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;

import org.ds.chronos.metrics.Metric;
import org.ds.chronos.timeline.TimelineEncoder;
import org.ds.chronos.util.Duration;

/**
 * An encoder for metrics of a known, fixed, interval. By grouping metrics into
 * a single column, we can take advantage of compression, and strip much of the
 * overhead associated with storing a column (13+ bytes). Reading in 5MM metrics
 * with the compacted decoder takes much less time.
 * <p>
 * Only use this encoder for archiving metrics of fixed interval.
 * 
 * TODO: Use duration boundaries and add domain filter to stream
 * 
 * @author Dan Simpson
 * 
 */
public class MetricArchiveEncoder implements TimelineEncoder<Metric> {

  private Iterator<Metric> input;

  private Duration window;
  private long windowSize;
  private int count = 0;
  private long time;
  private ByteBuffer buffer;
  private long interval;

  /**
   * Create a compacting metric encoder
   * 
   * @param interval
   *          the interval or period between each metric, eg: 1 minute
   * @param window
   *          the amount of time for which metrics can be stored. Example:
   *          window = new Duration("1h"), causes 1 hour of metrics to be
   *          grouped into a single column.
   */
  public MetricArchiveEncoder(Duration interval, Duration window) {
    this.interval = interval.getMillis();
    this.window = window;
    this.windowSize = window.getMillis();

    int bytes = (int) ((this.windowSize + this.interval - 1) / this.interval);
    this.buffer = ByteBuffer.allocate(bytes * Metric.BYTE_SIZE + 16);
  }

  @Override
  public boolean hasNext() {
    while (count++ * interval < windowSize && input.hasNext()) {
      Metric metric = input.next();
      if (count == 1) {
        time = window.justifyPastOrNow(metric.getTime());
        buffer.putLong(metric.getTime());
        buffer.putLong(interval);
      }
      buffer.putFloat(metric.getValue());
    }
    return count > 1;
  }

  @Override
  public HColumn<Long, byte[]> next() {
    byte[] data = new byte[buffer.position()];
    buffer.rewind();
    buffer.get(data);
    buffer.rewind();
    count = 0;
    return HFactory.createColumn(time, data);
  }

  @Override
  public void remove() {
  }

  @Override
  public void setInputStream(Iterator<Metric> input) {
    this.input = input;
  }

}
