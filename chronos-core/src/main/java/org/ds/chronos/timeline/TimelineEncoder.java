package org.ds.chronos.timeline;

import java.util.Iterator;

import me.prettyprint.hector.api.beans.HColumn;

/**
 * 
 * @author Dan Simpson
 *
 * @param <T>
 */
public interface TimelineEncoder<T> extends Iterator<HColumn<Long, byte[]>> {
	
	/**
	 * 
	 * @param input
	 */
	public void setInputStream(Iterator<T> input);
}
