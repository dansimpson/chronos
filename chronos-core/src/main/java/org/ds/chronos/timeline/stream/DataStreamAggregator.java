package org.ds.chronos.timeline.stream;

import java.util.Iterator;

public class DataStreamAggregator<I, O> implements DataStreamIterator<O>  {

	/**
	 * A aggregator for reducing upstream data for
	 * downstream processing.
	 * 
	 * 
	 * 
	 * @author Dan
	 *
	 * @param <I> input type
	 * @param <O> output type
	 */
	public static interface Aggregator<I, O> {
		
		/**
		 * Return true if full, meaning we will flush
		 * @param item
		 * @return
		 */
		public boolean feed(I item);
		
		/**
		 * Flush out the reduced item.  Return null to
		 * indicate that we are done here.
		 * @return object or null if nothing exists
		 */
		public O flush();
	}
	
	private Iterator<I> input;
	private Aggregator<I, O> aggregate;
	private O last;
	
	public DataStreamAggregator(Aggregator<I, O> aggregate) {
		this.aggregate = aggregate;
	}

	@Override
	public boolean hasNext() {
		while(input.hasNext()) {
			if(aggregate.feed(input.next())) {
				last = aggregate.flush();
				return true;
			}
		}
		
		if((last = aggregate.flush()) == null) {
			return false;
		}
		
		return true;
	}

	@Override
	public O next() {
		return last;
	}

	@Override
	public void remove() {
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public void setUpstream(Iterator<?> upstream) {
		input = (Iterator<I>)upstream;
	}
}