package org.ds.chronos.api;

import java.util.Iterator;


/**
 * 
 * @author Dan Simpson
 *
 * @param <T>
 */
public interface TimelineEncoder<T> extends Iterator<ChronologicalRecord> {
	
	/**
	 * 
	 * @param input
	 */
	public void setInputStream(Iterator<T> input);
}
