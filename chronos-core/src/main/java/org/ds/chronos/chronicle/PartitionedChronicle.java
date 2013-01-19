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
import me.prettyprint.hector.api.beans.HColumn;

import org.ds.chronos.api.Chronicle;
import org.ds.chronos.api.Chronos;
import org.ds.chronos.timeline.Timeline;

/**
 * 
 * PartitionedChronicle
 * <p>
 * A timeseries repository that stores events as columsn over
 * many keys.  This strategy is aimed at events which have
 * a predictable interval.
 * <p>
 * Each event is mapped, or paritioned on, a key which has
 * Date information.  For instance, if {@link PartitionPeriod} is
 * YEAR, then an event with time 2012-04-01 will partition onto
 * the key: prefix-2012.
 * <p>
 * @see Timeline
 * @see PartitionPeriod
 * @see Chronos#getChronicle(String, PartitionPeriod)
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
	public void add(HColumn<Long, byte[]> item) {
		new CassandraChronicle(keyspace, template, getKeyName(item.getName())).add(item);
	}

	/**
	 * Partition in memory and then batch write to each key
	 * @param columns
	 */
	@Override
	public void add(Iterator<HColumn<Long, byte[]>> columns, int pageSize) {
		Map<String, List<HColumn<Long, byte[]>>> groups = new HashMap<String, List<HColumn<Long, byte[]>>>();
		
		Calendar date = Calendar.getInstance();
		while(columns.hasNext()) {
			HColumn<Long, byte[]> column = columns.next();
			date.setTimeInMillis(column.getName());
			String key = getKeyName(date);
			
			if(!groups.containsKey(key)) {
				groups.put(key, new LinkedList<HColumn<Long, byte[]>>());
			}
			groups.get(key).add(column);
		}
		
		for(Entry<String, List<HColumn<Long, byte[]>>> entry: groups.entrySet()) {
			new CassandraChronicle(keyspace, template, entry.getKey()).add(entry.getValue());
		}
	}

	@Override
	public Iterator<HColumn<Long, byte[]>> getRange(long t1, long t2, int batchSize) {
		LinkedList<Iterator<HColumn<Long, byte[]>>> iterators = new LinkedList<Iterator<HColumn<Long, byte[]>>>();
		for(CassandraChronicle chronicle: getSubChronicles(t1, t2)) {
			iterators.add(chronicle.getRange(t1, t2, batchSize));
		}
		return new PartitionIterator(iterators);
	}


	@Override
	public long getNumEvents(long t1, long t2) {
		long result = 0;
		for(CassandraChronicle chronicle: getSubChronicles(t1, t2)) {
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
		for(CassandraChronicle chronicle: getSubChronicles(t1, t2)) {
			chronicle.deleteRange(t1, t2);
		}
	}

	private List<CassandraChronicle> getSubChronicles(long t1, long t2) {
		List<CassandraChronicle> items = new LinkedList<CassandraChronicle>();
		for(String key: period.getPeriodKeys(keyPrefix, t1, t2)) {
			items.add(new CassandraChronicle(keyspace, template, key));
		}
		return items;
	}
	
}
