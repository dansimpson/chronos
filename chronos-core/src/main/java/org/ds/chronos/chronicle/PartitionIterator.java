package org.ds.chronos.chronicle;

import java.util.Iterator;
import java.util.LinkedList;

import me.prettyprint.hector.api.beans.HColumn;

/**
 * An iterator of slice iterators used for traversing many columns on many rows.
 * 
 * @author Dan Simpson
 * 
 */
public class PartitionIterator implements Iterator<HColumn<Long, byte[]>> {

  private LinkedList<Iterator<HColumn<Long, byte[]>>> iterators;

  public PartitionIterator(LinkedList<Iterator<HColumn<Long, byte[]>>> iterators) {
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
  public HColumn<Long, byte[]> next() {
    return iterators.getFirst().next();
  }

  @Override
  public void remove() {
    iterators.getFirst().remove();
  }

}
