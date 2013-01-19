package org.ds.chronos.timeline.json;

/**
 * An interface which exposes a timestamp for a given object
 * 
 * @author Dan Simpson
 *
 */
public interface Timestamped {
	public long getTimestamp();
	public void setTimestamp(long timestamp);
}
