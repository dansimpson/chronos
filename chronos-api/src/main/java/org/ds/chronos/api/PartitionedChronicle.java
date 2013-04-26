package org.ds.chronos.api;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.ds.chronos.timeline.Timeline;

/**
 * 
 * PartitionedChronicle
 * <p>
 * A chronicle which stores data over many keys. This strategy is aimed at data with a predictable interval.
 * <p>
 * Each event is mapped to a partition. A parition is a key which has Date information. For instance, if {@link PartitionPeriod} is YEAR,
 * then an event with date 2012-04-01 will partition onto the key: prefix-2012.
 * <p>
 * 
 * @see Timeline
 * @see PartitionPeriod
 * 
 * TODO: Threaded prefetching?
 * 
 * @author Dan Simpson
 * 
 */
public abstract class PartitionedChronicle extends Chronicle {

	protected final String keyPrefix;
	protected final PartitionPeriod period;

	public PartitionedChronicle(String keyPrefix, PartitionPeriod period) {
		super();
		this.keyPrefix = keyPrefix;
		this.period = period;
	}

	/**
	 * Create a chronicle for a single partition
	 * 
	 * @param key
	 *          the key of the partition
	 * @return a chronicle which represents only the single partition
	 */
	protected abstract Chronicle getPartition(String key);

	@Override
	public void add(ChronologicalRecord item) {
		getPartition(getKeyName(item.getTimestamp())).add(item);
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
			getPartition(entry.getKey()).add(entry.getValue());
		}
	}

	@Override
	public Iterator<ChronologicalRecord> getRange(long t1, long t2, int batchSize) {
		LinkedList<Iterator<ChronologicalRecord>> iterators = new LinkedList<Iterator<ChronologicalRecord>>();
		for (Chronicle chronicle : getSubChronicles(t1, t2)) {
			iterators.add(chronicle.getRange(t1, t2, batchSize));
		}
		return new PartitionIterator(iterators);
	}

	@Override
	public long getNumEvents(long t1, long t2) {
		long result = 0;
		for (Chronicle chronicle : getSubChronicles(t1, t2)) {
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
		for (Chronicle chronicle : getSubChronicles(t1, t2)) {
			chronicle.deleteRange(t1, t2);
		}
	}

	private List<Chronicle> getSubChronicles(long t1, long t2) {
		List<Chronicle> items = new LinkedList<Chronicle>();
		for (String key : period.getPeriodKeys(keyPrefix, t1, t2)) {
			items.add(getPartition(key));
		}
		return items;
	}

}
