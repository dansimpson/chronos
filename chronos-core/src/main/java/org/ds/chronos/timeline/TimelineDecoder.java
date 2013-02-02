package org.ds.chronos.timeline;

import java.util.Iterator;

import me.prettyprint.hector.api.beans.HColumn;

/**
 * 
 * Implement this to allow streaming objects of your choosing, which are built
 * lazily for each page of data
 * 
 * @author Dan Simpson
 * 
 * @param <T>
 */
public interface TimelineDecoder<T> extends Iterator<T> {

  /**
   * 
   * @param input
   */
  public void setInputStream(Iterator<HColumn<Long, byte[]>> input);
}
