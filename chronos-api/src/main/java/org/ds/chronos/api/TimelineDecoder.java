package org.ds.chronos.api;

import java.util.Iterator;


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
  public void setInputStream(Iterator<ChronologicalRecord> input);
}
