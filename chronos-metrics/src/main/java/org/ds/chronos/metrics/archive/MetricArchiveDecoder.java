package org.ds.chronos.metrics.archive;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.ds.chronos.api.ChronologicalRecord;
import org.ds.chronos.api.TimelineDecoder;
import org.ds.chronos.metrics.Metric;

/**
 * 
 * A MetricDecoder which expects each column to hold large number of values,
 * spaced by a fixed interval.
 * 
 * @author Dan Simpson
 * 
 */
public class MetricArchiveDecoder implements TimelineDecoder<Metric> {

  private Iterator<ChronologicalRecord> input;

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
        ChronologicalRecord column = input.next();
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
  public void setInputStream(Iterator<ChronologicalRecord> input) {
    this.input = input;
  }

}
