package org.ds.chronos.timeline;

import java.util.Iterator;

import org.ds.chronos.api.ChronologicalRecord;

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
