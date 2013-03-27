package org.ds.chronos.timeline.stream;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.ds.chronos.util.JoiningIterator;

import com.google.common.base.Function;

/**
 * A join of N data streams (of the same class, or contract). This allows joining of multiple streams with a transform function.
 * 
 * @author Dan Simpson
 * 
 * @param <T>
 *          the input type of eachs tream
 * @param <O>
 *          the resulting type of the join
 */
public class DataStreamJoin<T, O> extends DataStream<O> {

	public DataStreamJoin(Collection<DataStream<T>> streams, Function<Iterable<T>, O> fn) {
		super(wrap(streams, fn));
	}

	private static <O, T> Iterable<O> wrap(final Collection<DataStream<T>> streams, final Function<Iterable<T>, O> fn) {
		return new Iterable<O>() {

			@Override
			public Iterator<O> iterator() {

				List<Iterator<T>> iterators = new ArrayList<Iterator<T>>();
				for (DataStream<T> stream : streams) {
					iterators.add(stream.stream().iterator());
				}

				return new JoiningIterator<T, O>(fn, iterators);
			}
		};
	}

}
