package org.ds.chronos.timeline;

import java.util.Iterator;

import org.ds.chronos.api.ChronologicalRecord;

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
