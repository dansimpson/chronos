package org.ds.chronos.timeline.json;

import org.ds.chronos.api.Temporal;

/**
 * An interface which exposes a timestamp for a given object
 * 
 * @author Dan Simpson
 * 
 */
public interface Timestamped extends Temporal {

	/**
	 * Set the timestamp
	 * 
	 * @param timestamp
	 *          unix timestamp in millis
	 */
	public void setTimestamp(long timestamp);
}
