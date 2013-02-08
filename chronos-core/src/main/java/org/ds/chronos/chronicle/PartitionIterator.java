package org.ds.chronos.chronicle;

import java.util.Iterator;
import java.util.LinkedList;

import org.ds.chronos.api.ChronologicalRecord;

/**
 * An iterator of slice iterators used for traversing many columns on many rows.
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
