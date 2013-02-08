package org.ds.chronos.chronicle;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.hector.api.Keyspace;

import org.ds.chronos.api.Chronicle;
import org.ds.chronos.api.ChronologicalRecord;
import org.ds.chronos.api.CassandraChronos;
import org.ds.chronos.timeline.Timeline;

/**
 * 
 * PartitionedChronicle
 * <p>
 * A timeseries repository that stores events as columsn over many keys. This
 * strategy is aimed at events which have a predictable interval.
 * <p>
 * Each event is mapped, or paritioned on, a key which has Date information. For
 * instance, if {@link PartitionPeriod} is YEAR, then an event with time
 * 2012-04-01 will partition onto the key: prefix-2012.
 * <p>
 * 
 * @see Timeline
 * @see PartitionPeriod
 * @see CassandraChronos#getChronicle(String, PartitionPeriod)
 * 
 * @author Dan Simpson
 * 
 */
public class PartitionedChronicle extends Chronicle {

  protected Keyspace keyspace;
  protected ColumnFamilyTemplate<String, Long> template;
  protected String keyPrefix;
  protected PartitionPeriod period;

  public PartitionedChronicle(Keyspace keyspace,
      ColumnFamilyTemplate<String, Long> template, String keyPrefix,
      PartitionPeriod period) {
    super();
    this.keyspace = keyspace;
    this.template = template;
    this.keyPrefix = keyPrefix;
    this.period = period;
  }

  @Override
  public void add(ChronologicalRecord item) {
    new CassandraChronicle(keyspace, template, getKeyName(item.getTimestamp()))
        .add(item);
  }

  /**
   * Partition in memory and then batch write to each key
   * 
   * @param columns
   */
  @Override
  public void add(Iterator<ChronologicalRecord> columns, int pageSize) {
    Map<String, List<ChronologicalRecord>> groups = new HashMap<String, List<ChronologicalRecord>>();

    Calendar date = Calendar.getInstance();
    while (columns.hasNext()) {
      ChronologicalRecord column = columns.next();
      date.setTimeInMillis(column.getTimestamp());
      String key = getKeyName(date);

      if (!groups.containsKey(key)) {
        groups.put(key, new LinkedList<ChronologicalRecord>());
      }
      groups.get(key).add(column);
    }

    for (Entry<String, List<ChronologicalRecord>> entry : groups.entrySet()) {
      new CassandraChronicle(keyspace, template, entry.getKey()).add(entry
          .getValue());
    }
  }

  @Override
  public Iterator<ChronologicalRecord> getRange(long t1, long t2, int batchSize) {
    LinkedList<Iterator<ChronologicalRecord>> iterators = new LinkedList<Iterator<ChronologicalRecord>>();
    for (CassandraChronicle chronicle : getSubChronicles(t1, t2)) {
      iterators.add(chronicle.getRange(t1, t2, batchSize));
    }
    return new PartitionIterator(iterators);
  }

  @Override
  public long getNumEvents(long t1, long t2) {
    long result = 0;
    for (CassandraChronicle chronicle : getSubChronicles(t1, t2)) {
      result += chronicle.getNumEvents(t1, t2);
    }
    return result;
  }

  @Override
  public void delete() {
  }

  private String getKeyName(long timestamp) {
    return period.getPeriodKey(keyPrefix, timestamp);
  }

  private String getKeyName(Calendar date) {
    return period.getPeriodKey(keyPrefix, date);
  }

  @Override
  public void deleteRange(long t1, long t2) {
    for (CassandraChronicle chronicle : getSubChronicles(t1, t2)) {
      chronicle.deleteRange(t1, t2);
    }
  }

  private List<CassandraChronicle> getSubChronicles(long t1, long t2) {
    List<CassandraChronicle> items = new LinkedList<CassandraChronicle>();
    for (String key : period.getPeriodKeys(keyPrefix, t1, t2)) {
      items.add(new CassandraChronicle(keyspace, template, key));
    }
    return items;
  }

}
