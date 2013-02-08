package org.ds.chronos.api;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.Iterator;

import org.ds.chronos.timeline.Timeline;

/**
 * 
 * Chronicle
 * <p>
 * A timeseries storage API with operations for storing columns, batches of
 * columns, fetching ranges, deleting ranges, and counting ranges. Events are
 * composed of a long timestamp value and a byte[] of data.
 * <p>
 * For a higher level of abstraction, see {@link Timeline}
 * <p>
 * 
 * @see CassandraChronicle
 * @see PartitionedChronicle
 * @see Chronos#getChronicle(String)
 * @see Chronos#getChronicle(String, PartitionPeriod)
 * 
 * @author Dan Simpson
 * 
 */
public abstract class Chronicle {

  public static final Charset CHARSET = Charset.forName("UTF8");
  public static final int WRITE_PAGE_SIZE = 1024;
  public static final int READ_PAGE_SIZE = 2048;

  /**
   * Add a single column to the Chronicle
   * 
   * @param column
   */
  public abstract void add(ChronologicalRecord item);

  /**
   * Add columns in batches
   * 
   * @param items
   *          columns
   * @param pageSize
   *          The number of columns to write per batch
   */
  public abstract void add(Iterator<ChronologicalRecord> items, int pageSize);

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
    add(new ChronologicalRecord(timestamp, data.getBytes(CHARSET)));
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

  /**
   * 
   * @param items
   */
  public void add(Iterator<ChronologicalRecord> items) {
    add(items, WRITE_PAGE_SIZE);
  }

  /**
   * 
   * @param items
   */
  public void add(Iterable<ChronologicalRecord> items) {
    add(items.iterator(), WRITE_PAGE_SIZE);
  }

  /**
   * 
   * @param items
   */
  public void add(ChronicleBatch batch) {
    add(batch.iterator(), WRITE_PAGE_SIZE);
  }

  /**
   * Get lazy loading stream of events. If t1 > t2, then the list will be
   * iterated in reverse.
   * 
   * @param t1
   *          begin time
   * @param t1
   *          end time
   * @param pageSize
   *          the number of columns to fetch per batch
   * @return
   */
  public abstract Iterator<ChronologicalRecord> getRange(long t1, long t2,
      int pageSize);

  /**
   * 
   * Get lazy loaded range of events
   * 
   * @param t1
   * @param t2
   * @return
   */
  public Iterator<ChronologicalRecord> getRange(long t1, long t2) {
    return getRange(t1, t2, READ_PAGE_SIZE);
  }

  /**
   * 
   * Get lazy loaded range of events
   * 
   * @param beginTime
   * @param endTime
   * @param pageSize
   * @return
   */
  public Iterator<ChronologicalRecord> getRange(Date beginTime, Date endTime) {
    return getRange(beginTime.getTime(), endTime.getTime());
  }

  /**
   * 
   * Get lazy loaded range of events
   * 
   * @param beginTime
   * @param endTime
   * @param pageSize
   *          number of columns to fetch at a time
   * @return
   */
  public Iterator<ChronologicalRecord> getRange(Date beginTime, Date endTime,
      int pageSize) {
    return getRange(beginTime.getTime(), endTime.getTime(), pageSize);
  }

  /**
   * Get the number of events for a given time range
   * 
   * @return number of columns accross all keys
   */
  public abstract long getNumEvents(long t1, long t2);

  public boolean isEventRecorded(long t1) {
    return getNumEvents(t1, t1) > 0;
  }

  /**
   * 
   * @param d1
   * @param d2
   * @return the number of events between d1 and d2
   */
  public long getNumEvents(Date d1, Date d2) {
    return getNumEvents(d1.getTime(), d2.getTime());
  }

  /**
   * Delete the entire chronicle
   */
  public abstract void delete();

  /**
   * Delete a subset of the Chronicle
   * 
   * @param beginTime
   * @param endTime
   */
  public abstract void deleteRange(long t1, long t2);

  public void deleteRange(Date t1, Date t2) {
    deleteRange(t1.getTime(), t2.getTime());
  }
}
