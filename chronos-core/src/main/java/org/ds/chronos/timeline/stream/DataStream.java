package org.ds.chronos.timeline.stream;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.ds.chronos.timeline.stream.DataStreamAggregator.Aggregator;
import org.ds.chronos.timeline.stream.DataStreamFilter.FilterFn;
import org.ds.chronos.timeline.stream.DataStreamMap.MapFn;
import org.ds.chronos.timeline.stream.DataStreamTransform.TransformFn;


@SuppressWarnings("unchecked")
/**
 * Lazy iterator chain for processing data on the fly.  It's
 * essentially a pipeline.
 * 
 * @author Dan
 *
 * @param <O> The output type after all transformations
 */
public class DataStream<O> {

	public LinkedList<Iterator<?>> stages = new LinkedList<Iterator<?>>();
	
	public DataStream(Iterator<?> source) {
		stages.add(source);
	}
	
	private DataStream<O> addStage(DataStreamIterator<?> stage) {
		if(!stages.isEmpty()) {
			stage.setUpstream(stages.getLast());
		}
		stages.addLast(stage);
		return this;
	}
	
	/**
	 * Apply a filter function
	 * @param fn
	 * @return self
	 */
	public <T> DataStream<O> filter(final FilterFn<T> fn) {
		return addStage(new DataStreamFilter<T>(fn));
	}
	
	/**
	 * Apply a map function
	 * @param fn
	 * @return self
	 */
	public <T,K> DataStream<O> map(final MapFn<T,K> fn) {
		return addStage(new DataStreamMap<T,K>(fn));
	}
	
	/**
	 * Apply a transform function
	 * @param fn
	 * @return self
	 */
	public <T> DataStream<O> transform(final TransformFn<T> fn) {
		return addStage(new DataStreamTransform<T>(fn));
	}
	
	/**
	 * Apply an aggregation action
	 * @param agg
	 * @return self
	 */
	public <T,K> DataStream<O> aggregate(final Aggregator<T,K> agg) {
		return addStage(new DataStreamAggregator<T,K>(agg));
	}
	
	/**
	 * The end of the lazy loading iterator chain
	 * 
	 * @return the iterator of the last item
	 */
	public Iterator<O> iterator() {
		return (Iterator<O>)stages.getLast();
	}
	
	/**
	 * 
	 * Create an interable for lazy streaming
	 * 
	 * @return stream which lazy iterates
	 */
	public Iterable<O> stream() {
		final Iterator<O> iterator = iterator();
		return new Iterable<O>() {
			public Iterator<O> iterator() {
				return iterator;
			}
		};
	}
	
	/**
	 * Iterate over the results and produce a list
	 * 
	 * @return A list of query results.  
	 */
	public List<O> list() {
		List<O> list = new ArrayList<O>();
		Iterator<O> iter = iterator();
		while(iter.hasNext()) {
			list.add(iter.next());
		}
		return list;
	}
	
	/**
	 * Return the first or only item of the result
	 * 
	 * @return The result
	 */
	public O first() {
		return iterator().hasNext() ? iterator().next() : null;
	}
	
}
