package org.ds.chronos.metrics.archive;

import java.nio.ByteBuffer;
import java.util.Iterator;

import me.prettyprint.hector.api.beans.HColumn;

import org.ds.chronos.metrics.Metric;
import org.ds.chronos.timeline.TimelineDecoder;

/**
 * 
 * A MetricDecoder which expects each column to hold large number of values,
 * spaced by a fixed interval.
 * 
 * @author Dan Simpson
 * 
 */
public class MetricArchiveDecoder implements TimelineDecoder<Metric> {

  private Iterator<HColumn<Long, byte[]>> input;

  private long time;
  private long interval;
  private int offset;
  private ByteBuffer buffer;

  public MetricArchiveDecoder() {
  }

  @Override
  public boolean hasNext() {
    if (buffer == null || !buffer.hasRemaining()) {
      if (input.hasNext()) {
        HColumn<Long, byte[]> column = input.next();
        buffer = column.getValueBytes();
        time = buffer.getLong();
        interval = buffer.getLong();
        offset = 0;
      } else {
        return false;
      }
    }

    return true;
  }

  @Override
  public Metric next() {
    return new Metric(time + (offset++ * interval), buffer.getFloat());
  }

  @Override
  public void remove() {
  }

  @Override
  public void setInputStream(Iterator<HColumn<Long, byte[]>> input) {
    this.input = input;
  }

}
