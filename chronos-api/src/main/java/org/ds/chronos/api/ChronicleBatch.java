package org.ds.chronos.api;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Utility for batch updates of events
 * 
 * @author Dan Simpson
 * 
 */
public class ChronicleBatch {

  private List<ChronologicalRecord> columns = new ArrayList<ChronologicalRecord>();

  public ChronicleBatch() {
  }

  /**
   * Batch add columns
   * 
   * @param items
   */
  public void add(ChronologicalRecord item) {
    columns.add(item);
  }
  
  /**
   * Add data with a given time stamp
   * 
   * @param timestamp
   * @param data
   */
  public void add(long timestamp, byte[] data) {
    add(new ChronologicalRecord(timestamp, data));
  }

  /**
   * Add byte buffer with a given time stamp
   * 
   * @param timestamp
   * @param data
   */
  public void add(long timestamp, ByteBuffer data) {
    add(new ChronologicalRecord(timestamp, data.array()));
  }

  /**
   * Add string bytes with a given time stamp
   * 
   * @param timestamp
   * @param data
   */
  public void add(long timestamp, String data) {
    add(new ChronologicalRecord(timestamp, data.getBytes(Chronicle.CHARSET)));
  }

  /**
   * Add data with a given date time
   * 
   * @param time
   * @param data
   */
  public void add(Date time, byte[] data) {
    add(time.getTime(), data);
  }

  /**
   * Add data with a given date time
   * 
   * @param time
   * @param data
   */
  public void add(Date time, ByteBuffer data) {
    add(time.getTime(), data);
  }

  /**
   * Add data with a given date time
   * 
   * @param time
   * @param data
   */
  public void add(Date time, String data) {
    add(time.getTime(), data);
  }

  public Iterator<ChronologicalRecord> iterator() {
    return columns.iterator();
  }
}
