package org.ds.chronos.timeline.stream.partitioned;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.ds.chronos.timeline.stream.DataStream;

import com.google.common.base.Function;

/**
 * A partitioned data stream which leverages a predicate to determine if a given item in the stream belongs to the "active" partition. The
 * goal is to convert a large data stream into a stream of streams. This gives us the ability to partition, then mapReduce the results in a
 * streaming fashion
 * 
 * TODO: We can do this without buffering the objects in an array
 * 
 * @author Dan Simpson
 * 
 * @param <T>
 */
public class PartitionedDataStream<T> extends DataStream<DataStream<T>> {

	/**
	 * 
	 * @param stream
	 *          the stream to partition
	 * @param boundary
	 *          the boundary predicate. When returning false, a new parition will start and the active partition will be passed downstream
	 */
	public PartitionedDataStream(DataStream<T> stream, final PartitionPredicate<? super T> boundary) {
		super(wrap(stream.stream().iterator(), boundary));
	}

	private static <T> Iterable<DataStream<T>> wrap(final Iterator<T> source, final PartitionPredicate<? super T> boundary) {
		return new Iterable<DataStream<T>>() {

			private T last;

			@Override
			public Iterator<DataStream<T>> iterator() {
				return new Iterator<DataStream<T>>() {

					@Override
					public boolean hasNext() {
						return source.hasNext();
					}

					@Override
					public DataStream<T> next() {

						List<T> buffer = new ArrayList<T>();

						if (last == null) {
							last = source.next();
						}

						while (boundary.apply(last)) {
							buffer.add(last);

							if (!hasNext()) {
								break;
							}
							last = source.next();
						}

						boundary.advance();

						return new DataStream<T>(buffer);
					}

					@Override
					public void remove() {
					}

				};
			}

		};
	}

	/**
	 * Reduce each stream with a given reduce function, emitting a stream of reduced objects
	 * 
	 * @param fn
	 *          reduce function
	 * @return
	 */
	public <K> DataStream<K> reduceAll(final Function<Iterable<T>, K> fn) {

		final Iterator<DataStream<T>> source = stream().iterator();

		return new DataStream<K>(new Iterable<K>() {

			@Override
			public Iterator<K> iterator() {
				return new Iterator<K>() {

					@Override
					public boolean hasNext() {
						return source.hasNext();
					}

					@Override
					public K next() {
						return source.next().reduce(fn);
					}

					@Override
					public void remove() {
					}
				};
			}
		});
	}

}
