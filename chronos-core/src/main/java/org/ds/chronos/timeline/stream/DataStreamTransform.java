package org.ds.chronos.timeline.stream;

/**
 * Map class which returns the same object, possibly
 * modified
 * 
 * @author Dan
 *
 * @param <T> The class to transform from to
 */
public class DataStreamTransform<T> extends DataStreamMap<T, T> {

	/**
	 * A transform that works similar to an identity map
	 * 
	 * @author Dan
	 *
	 * @param <T>
	 */
	public static interface TransformFn<T> extends MapFn<T, T> {}
	
	/**
	 * Create transformer for a given transform function
	 * @param transformer
	 */
	public DataStreamTransform(TransformFn<T> transformer) {
		super(transformer);
	}
	
}