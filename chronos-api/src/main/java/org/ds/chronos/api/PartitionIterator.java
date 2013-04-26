package org.ds.chronos.api;

import java.util.Iterator;
import java.util.LinkedList;


/**
 * An iterator of iterators used for flattening blocks
 * 
 * @author Dan Simpson
 * 
 */
public class PartitionIterator implements Iterator<ChronologicalRecord> {

	private LinkedList<Iterator<ChronologicalRecord>> iterators;

	public PartitionIterator(LinkedList<Iterator<ChronologicalRecord>> iterators) {
		this.iterators = iterators;
	}

	@Override
	public boolean hasNext() {
		while (!iterators.isEmpty()) {
			if (iterators.getFirst().hasNext()) {
				return true;
			}
			iterators.removeFirst();
		}
		return false;
	}

	@Override
	public ChronologicalRecord next() {
		return iterators.getFirst().next();
	}

	@Override
	public void remove() {
		iterators.getFirst().remove();
	}

}
