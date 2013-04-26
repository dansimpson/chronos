package org.ds.chronos.api;

import java.nio.ByteBuffer;

/**
 * A single Chronological Record
 * 
 * @author Dan Simpson
 * 
 */
public class ChronologicalRecord implements Comparable<ChronologicalRecord> {

	private long timestamp;
	private byte[] data;

	public ChronologicalRecord(long timestamp, byte[] data) {
		super();
		this.timestamp = timestamp;
		this.data = data;
	}

	/**
	 * @return the timestamp
	 */
	public long getTimestamp() {
		return timestamp;
	}

	/**
	 * @return the data
	 */
	public byte[] getData() {
		return data;
	}

	public ByteBuffer getValueBytes() {
		return ByteBuffer.wrap(data);
	}

	@Override
	public int compareTo(ChronologicalRecord o) {
		return Long.valueOf(timestamp).compareTo(o.timestamp);
	}

	/**
	 * Get the number of bytes in the record
	 * 
	 * @return
	 */
	public int getByteSize() {
		return data.length;
	}

}
