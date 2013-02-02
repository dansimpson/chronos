package org.ds.chronos.timeline.json;

/**
 * An interface which exposes a timestamp for a given object
 * 
 * @author Dan Simpson
 * 
 */
public interface Timestamped {

  /**
   * Get the timestamp, as unix timestamp in millis since epoch
   * 
   * @return the timestamp of the object
   */
  public long getTimestamp();

  /**
   * Set the timestamp
   * 
   * @param timestamp
   *          unix timestamp in millis
   */
  public void setTimestamp(long timestamp);
}
