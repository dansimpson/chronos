package org.ds.chronos.chronicle;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;

import org.ds.chronos.api.Chronicle;
import org.ds.chronos.api.Chronos;

/**
 * A memory based Chronicle for tests.
 * 
 * @author Dan
 *
 */
public class MemoryChronicle extends Chronicle {

	/**
	 * Little wrapper with comparison...
	 * @author Dan
	 *
	 */
	private class Wrapped implements Comparable<Wrapped> {
		
		public HColumn<Long,byte[]> inside;
		
		public Wrapped(HColumn<Long, byte[]> inside) {
			super();
			this.inside = inside;
		}
		
		public Wrapped(Long time, byte[] data) {
			this(HFactory.createColumn(time, data));
		}
		
		@Override
		public int compareTo(Wrapped o) {
			return inside.getName().compareTo(o.inside.getName());
		}
	}
	
	private TreeSet<Wrapped> items = new TreeSet<Wrapped>();
	
	public MemoryChronicle() {
	}
	
	@Override
	public void add(HColumn<Long, byte[]> item) {
		items.add(new Wrapped(item));
	}

	@Override
	public void add(Iterator<HColumn<Long, byte[]>> itr, int pageSize) {
		while(itr.hasNext()) {
			items.add(new Wrapped(itr.next()));
		}
	}

	@Override
	public Iterator<HColumn<Long, byte[]>> getRange(long t1, long t2, int pageSize) {
		if(t1 > t2) {
			return getRangeReversed(t2, t1);
		}
		List<HColumn<Long, byte[]>> result = new LinkedList<HColumn<Long, byte[]>>();
		for(Wrapped item: items.subSet(low(t1), true, high(t2), true)) {
			result.add(item.inside);
		}
		return result.iterator();
	}
	
	private Iterator<HColumn<Long, byte[]>> getRangeReversed(long t1, long t2) {
		LinkedList<HColumn<Long, byte[]>> result = new LinkedList<HColumn<Long, byte[]>>();
		for(Wrapped item: items.subSet(low(t1), true, high(t2), true)) {
			result.addFirst(item.inside);
		}
		return result.iterator();
	}
	
	@Override
	public long getNumEvents(long t1, long t2) {
		return items.subSet(low(t1), true, high(t2), true).size();
	}

	@Override
	public void delete() {
		items.clear();
	}

	@Override
	public void deleteRange(long t1, long t2) {
		items.removeAll(Chronos.toList(getRange(t1, t2)));
	}
	
	private Wrapped low(long time) {
		return items.ceiling(new Wrapped(time, new byte[0]));
	}
	
	private Wrapped high(long time) {
		return items.floor(new Wrapped(time, new byte[0]));
	}

}