package org.ds.chronos.timeline.stream;

import java.util.Iterator;

public class DataStreamAggregator<I, O> implements DataStreamIterator<O> {

	/**
	 * A aggregator for reducing upstream data for downstream processing.
	 * 
	 * 
	 * 
	 * @author Dan Simpson
	 * 
	 * @param <I>
	 *          input type
	 * @param <O>
	 *          output type
	 */
	public static interface Aggregator<I, O> {

		/**
		 * Add an item to the aggregator
		 * 
		 * @param item
		 */
		public void add(I item);

		/**
		 * If the aggregator has a result, return true
		 * 
		 * @return
		 */
		public boolean hasResult();

		/**
		 * Flush out the reduced item. Return null to indicate that we are done here.
		 * 
		 * @return object or null if nothing exists
		 */
		public O getResult();
	}

	private Iterator<I> input;
	private Aggregator<I, O> aggregate;
	private O last;

	public DataStreamAggregator(Aggregator<I, O> aggregate) {
		this.aggregate = aggregate;
	}

	@Override
	public boolean hasNext() {
		while (input.hasNext()) {
			aggregate.add(input.next());
			if (aggregate.hasResult()) {
				last = aggregate.getResult();
				return true;
			}
		}

		if ((last = aggregate.getResult()) == null) {
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
		input = (Iterator<I>) upstream;
	}
}