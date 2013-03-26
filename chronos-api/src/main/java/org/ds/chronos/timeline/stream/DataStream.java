package org.ds.chronos.timeline.stream;

import java.util.Collection;

import org.ds.chronos.timeline.stream.partitioned.BucketSizePredicate;
import org.ds.chronos.timeline.stream.partitioned.PartitionPredicate;
import org.ds.chronos.timeline.stream.partitioned.PartitionedDataStream;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Lazy stream processing, with support for filters, transforms, partitioning, and reducing.
 * 
 * @author Dan Simpson
 * 
 * @param <O>
 *          The input type
 */
public class DataStream<O> {

	private final FluentIterable<O> source;

	public DataStream(Iterable<O> source) {
		this.source = FluentIterable.from(source);
	}

	/**
	 * Apply a filter condition
	 * 
	 * @param condition
	 *          the condition, which returns true if the object should pass
	 * @return new stream with filter applied
	 */
	public DataStream<O> filter(final Predicate<O> condition) {
		return new DataStream<O>(source.filter(condition));
	}

	/**
	 * Apply a map function to each element and emit a stream of mapped objects
	 * 
	 * @param fn
	 *          the map function
	 * @return new stream
	 */
	public <T> DataStream<T> map(final Function<O, T> fn) {
		return new DataStream<T>(source.transform(fn));
	}

	/**
	 * Reduce the stream into a single object
	 * 
	 * @param fn
	 *          the reduction function
	 * @return resulting object
	 */
	public <T> T reduce(final Function<Iterable<O>, T> fn) {
		return fn.apply(source);
	}

	/**
	 * Partition the stream into sub streams based on a boundary predicate.
	 * 
	 * @param predicate
	 *          the predicate which determines if a value belongs in a given parition
	 * @return
	 */
	public PartitionedDataStream<O> partition(final PartitionPredicate<O> predicate) {
		return new PartitionedDataStream<O>(this, predicate);
	}

	/**
	 * Partition the stream into buckets of size bucketSize
	 * 
	 * @param bucketSize
	 *          the number of elements to be returned per bucket
	 * @return
	 */
	public PartitionedDataStream<O> partition(int bucketSize) {
		return partition(new BucketSizePredicate<O>(bucketSize));
	}

	/**
	 * 
	 * Create an interable for lazy streaming
	 * 
	 * @return stream which lazy iterates
	 */
	public Iterable<O> stream() {
		return source;
	}

	@SuppressWarnings("unchecked")
	/**
	 * Stream list downcasted
	 * 
	 * TODO: Seems broken
	 * 
	 * @param klass
	 * @return
	 */
	public <T> Iterable<T> streamAs(Class<T> klass) {
		return map(new Function<O, T>() {

			public T apply(O o) {
				return (T) o;
			}
		}).stream();
	}

	/**
	 * @param predicate
	 * @return
	 * @see com.google.common.collect.FluentIterable#anyMatch(com.google.common.base.Predicate)
	 */
	public final boolean anyMatch(Predicate<? super O> predicate) {
		return source.anyMatch(predicate);
	}

	/**
	 * @param predicate
	 * @return
	 * @see com.google.common.collect.FluentIterable#allMatch(com.google.common.base.Predicate)
	 */
	public final boolean allMatch(Predicate<? super O> predicate) {
		return source.allMatch(predicate);
	}

	/**
	 * @param predicate
	 * @return
	 * @see com.google.common.collect.FluentIterable#firstMatch(com.google.common.base.Predicate)
	 */
	public final Optional<O> firstMatch(Predicate<? super O> predicate) {
		return source.firstMatch(predicate);
	}

	/**
	 * @return
	 * @see com.google.common.collect.FluentIterable#first()
	 */
	public final Optional<O> first() {
		return source.first();
	}

	/**
	 * @return
	 * @see com.google.common.collect.FluentIterable#last()
	 */
	public final Optional<O> last() {
		return source.last();
	}

	/**
	 * @param size
	 * @return
	 * @see com.google.common.collect.FluentIterable#limit(int)
	 */
	public final FluentIterable<O> limit(int size) {
		return source.limit(size);
	}

	/**
	 * @return
	 * @see com.google.common.collect.FluentIterable#toList()
	 */
	public final ImmutableList<O> toList() {
		return source.toList();
	}

	/**
	 * @return
	 * @see com.google.common.collect.FluentIterable#toSet()
	 */
	public final ImmutableSet<O> toSet() {
		return source.toSet();
	}

	/**
	 * @param valueFunction
	 * @return
	 * @see com.google.common.collect.FluentIterable#toMap(com.google.common.base.Function)
	 */
	public final <V> ImmutableMap<O, V> toMap(Function<? super O, V> valueFunction) {
		return source.toMap(valueFunction);
	}

	/**
	 * @param keyFunction
	 * @return
	 * @see com.google.common.collect.FluentIterable#index(com.google.common.base.Function)
	 */
	public final <K> ImmutableListMultimap<K, O> index(Function<? super O, K> keyFunction) {
		return source.index(keyFunction);
	}

	/**
	 * @param keyFunction
	 * @return
	 * @see com.google.common.collect.FluentIterable#uniqueIndex(com.google.common.base.Function)
	 */
	public final <K> ImmutableMap<K, O> uniqueIndex(Function<? super O, K> keyFunction) {
		return source.uniqueIndex(keyFunction);
	}

	/**
	 * @param type
	 * @return
	 * @see com.google.common.collect.FluentIterable#toArray(java.lang.Class)
	 */
	public final O[] toArray(Class<O> type) {
		return source.toArray(type);
	}

	/**
	 * @param collection
	 * @return
	 * @see com.google.common.collect.FluentIterable#copyInto(java.util.Collection)
	 */
	public final <C extends Collection<? super O>> C copyInto(C collection) {
		return source.copyInto(collection);
	}

	/**
	 * @return
	 * @see com.google.common.collect.FluentIterable#size()
	 */
	public final int size() {
		return source.size();
	}

}
