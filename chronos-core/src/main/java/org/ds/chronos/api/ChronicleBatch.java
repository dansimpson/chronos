package org.ds.chronos.api;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;

/**
 * Utility for batch updates of events
 * 
 * @author Dan Simpson
 *
 */
public class ChronicleBatch {

	private List<HColumn<Long, byte[]>> columns = new ArrayList<HColumn<Long, byte[]>>();
	
	public ChronicleBatch() {
	}
	
	/**
	 * Batch add columns
	 * @param items
	 */
	public void add(HColumn<Long, byte[]> item) {
		columns.add(item);
	}

	/**
	 * Add data with a given time stamp
	 * @param timestamp
	 * @param data
	 */
	public void add(long timestamp, byte[] data) {
		add(HFactory.createColumn(timestamp, data));
	}
	
	/**
	 * Add byte buffer with a given time stamp
	 * @param timestamp
	 * @param data
	 */
	public void add(long timestamp, ByteBuffer data) {
		add(HFactory.createColumn(timestamp, data.array()));
	}
	
	/**
	 * Add string bytes with a given time stamp
	 * @param timestamp
	 * @param data
	 */
	public void add(long timestamp, String data) {
		add(HFactory.createColumn(timestamp, data.getBytes(Chronicle.CHARSET)));
	}
	
	/**
	 * Add data with a given date time
	 * @param time
	 * @param data
	 */
	public void add(Date time, byte[] data) {
		add(time.getTime(), data);
	}
	
	/**
	 * Add data with a given date time
	 * @param time
	 * @param data
	 */
	public void add(Date time, ByteBuffer data) {
		add(time.getTime(), data);
	}
	
	/**
	 * Add data with a given date time
	 * @param time
	 * @param data
	 */
	public void add(Date time, String data) {
		add(time.getTime(), data);
	}
	
	public Iterator<HColumn<Long, byte[]>> iterator() {
		return columns.iterator();
	}
}
