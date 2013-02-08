package org.ds.chronos.timeline.stream;

import java.util.Iterator;

public class DataStreamMap<I, O> implements DataStreamIterator<O> {

  /**
   * A function which converts an input to an output in a data stream.
   * 
   * @author Dan Simpson
   * 
   * @param <I>
   *          input or upstream data type
   * @param <O>
   *          output or downstream data type
   */
  public static interface MapFn<I, O> {

    public O map(I item);
  }

  private Iterator<I> input;
  private MapFn<I, O> mapper;

  public DataStreamMap(MapFn<I, O> mapper) {
    this.mapper = mapper;
  }

  @Override
  public boolean hasNext() {
    return input.hasNext();
  }

  @Override
  public O next() {
    return mapper.map(input.next());
  }

  @Override
  public void remove() {
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setUpstream(Iterator<?> upstream) {
    input = (Iterator<I>) upstream;
  }
}