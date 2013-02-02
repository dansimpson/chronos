package org.ds.chronos.timeline.stream;

import java.util.Iterator;

public class DataStreamFilter<T> implements DataStreamIterator<T> {

  /**
   * A data stream filter check
   * 
   * @author Dan Simpson
   * 
   * @param <T>
   */
  public static interface FilterFn<T> {

    public boolean check(T item);
  }

  private T item;
  private Iterator<T> input;
  private FilterFn<T> filter;

  public DataStreamFilter(FilterFn<T> filter) {
    this.filter = filter;
  }

  @Override
  public boolean hasNext() {
    if (item != null) {
      return true;
    }

    while (input.hasNext()) {
      T tmp = input.next();
      if (filter.check(tmp)) {
        item = tmp;
        return true;
      }
    }
    return false;
  }

  @Override
  public T next() {
    T tmp = item;
    item = null;
    return tmp;
  }

  @Override
  public void remove() {
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setUpstream(Iterator<?> upstream) {
    input = (Iterator<T>) upstream;
  }
}
