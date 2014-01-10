package org.ds.chronos.api.chronicle;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

import org.ds.chronos.api.Chronicle;
import org.ds.chronos.api.ChronologicalRecord;

/**
 * A memory based Chronicle for tests.
 * 
 * @author Dan Simpson
 * 
 */
public class MemoryChronicle extends Chronicle {

	protected TreeSet<ChronologicalRecord> items = new TreeSet<ChronologicalRecord>();

	public MemoryChronicle() {
	}

	@Override
	public void add(ChronologicalRecord item) {
		items.add(item);
	}

	@Override
	public void add(Iterator<ChronologicalRecord> itr, int pageSize) {
		while (itr.hasNext()) {
			items.add(itr.next());
		}
	}

	@Override
	public Iterator<ChronologicalRecord> getRange(long t1, long t2, int pageSize) {
		if (t1 > t2) {
			return getRangeReversed(t2, t1);
		}
		List<ChronologicalRecord> result = new LinkedList<ChronologicalRecord>();
		for (ChronologicalRecord item : items.subSet(low(t1), true, high(t2), true)) {
			result.add(item);
		}
		return result.iterator();
	}

	private Iterator<ChronologicalRecord> getRangeReversed(long t1, long t2) {
		LinkedList<ChronologicalRecord> result = new LinkedList<ChronologicalRecord>();
		for (ChronologicalRecord item : items.subSet(low(t1), true, high(t2), true)) {
			result.addFirst(item);
		}
		return result.iterator();
	}

	@Override
	public long getNumEvents(long t1, long t2) {
		if (items.isEmpty()) {
			return 0;
		}
		try {
			return items.subSet(low(t1), true, high(t2), true).size();
		} catch (Throwable t) {
			return 0;
		}
	}

	@Override
	public void delete() {
		items.clear();
	}

	@Override
	public void deleteRange(long t1, long t2) {
		items.removeAll(toList(getRange(t1, t2)));
	}

	private ChronologicalRecord low(long time) {
		return items.ceiling(new ChronologicalRecord(time, new byte[0]));
	}

	private ChronologicalRecord high(long time) {
		return items.floor(new ChronologicalRecord(time, new byte[0]));
	}

	public static List<ChronologicalRecord> toList(Iterator<ChronologicalRecord> records) {
		List<ChronologicalRecord> result = new ArrayList<ChronologicalRecord>();
		while (records.hasNext()) {
			result.add(records.next());
		}
		return result;
	}

	public int size() {
		return items.size();
	}

	public TreeSet<ChronologicalRecord> all() {
		return items;
	}

	public boolean isEmpty() {
		return items.isEmpty();
	}

}