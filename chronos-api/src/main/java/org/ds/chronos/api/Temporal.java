package org.ds.chronos.api;

/**
 * A contract for an object to have a temporal attribute
 * 
 * @author Dan Simpson
 * 
 */
public interface Temporal {

	/**
	 * The timestamp for the temporal object
	 * 
	 * @return millis since epoch
	 */
	public long getTimestamp();

}
