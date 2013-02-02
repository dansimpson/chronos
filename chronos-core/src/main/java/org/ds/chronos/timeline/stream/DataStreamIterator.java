package org.ds.chronos.timeline.stream;

import java.util.Iterator;

/**
 * A special iterator for the DataStream which depends on the upstream iterator.
 * 
 * @author Dan Simpson
 * 
 * @param <T>
 */
public interface DataStreamIterator<T> extends Iterator<T> {

  /**
   * Set the upstream iterator as a data source for downstream requests.
   * 
   * @param upstream
   */
  public void setUpstream(Iterator<?> upstream);
}
