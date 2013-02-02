package org.ds.chronos.metrics;

import java.nio.ByteBuffer;

/**
 * Metric
 * <p>
 * A simple wrapper for a timeseries item with a numeric value.
 * 
 * @author Dan Simpson
 * 
 */
public class Metric {

  public static final int BYTE_SIZE = 4;

  private long time;
  private float value;

  public Metric(long time, float value) {
    this.time = time;
    this.value = value;
  }

  /**
   * @return the time
   */
  public long getTime() {
    return time;
  }

  /**
   * @param time
   *          the time to set
   */
  public void setTime(long time) {
    this.time = time;
  }

  /**
   * @return the value
   */
  public float getValue() {
    return value;
  }

  /**
   * @param value
   *          the value to set
   */
  public void setValue(float value) {
    this.value = value;
  }

  /**
	 * 
	 */
  public String toString() {
    return String.format("%d = %f", time, value);
  }

  public ByteBuffer toBuffer() {
    ByteBuffer buffer = ByteBuffer.allocate(BYTE_SIZE);
    buffer.putFloat(value);
    return buffer;
  }

}
