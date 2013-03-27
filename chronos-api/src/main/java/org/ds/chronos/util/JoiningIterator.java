package org.ds.chronos.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Function;


/**
 * Joining Iterator (AKA Joinerator). Takes a series of iterators, and joins them, and allows iterating on that joined result.
 * 
 * If there are 2 input iterators A, and B, the sequence of iteration is: A, B, A, B... A, B etc. This is not a concatenating iterator
 * 
 * @author Dan Simpson
 * 
 * @param <T>
 *          The input type of each iterator
 * @param <K>
 *          The output type of the Join
 */
public class JoiningIterator<T, K> implements Iterator<K> {

	final Iterable<Iterator<T>> streams;
	final Function<Iterable<T>, K> fn;

	/**
	 * 
	 * @param joinFn
	 *          A function which takes a List of T objects, and should produce a K object
	 * @param streams
	 *          the streams which we join
	 */
	public JoiningIterator(Function<Iterable<T>, K> joinFn, Iterable<Iterator<T>> streams) {
		super();
		this.fn = joinFn;
		this.streams = streams;
	}

	@Override
	public boolean hasNext() {
		boolean result = true;
		for (Iterator<T> stream : streams) {
			result &= stream.hasNext();
		}
		return result;
	}

	@Override
	public K next() {
		List<T> input = new ArrayList<T>();
		for (Iterator<T> stream : streams) {
			input.add(stream.next());
		}
		return fn.apply(input);
	}

	@Override
	public void remove() {
	}

}