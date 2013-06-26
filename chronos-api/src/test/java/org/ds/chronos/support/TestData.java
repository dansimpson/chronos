package org.ds.chronos.support;

import org.ds.chronos.api.Temporal;

public class TestData implements Temporal, Comparable<Temporal> {

	public long time;
	public byte type;
	public double value;

	public TestData() {
	}

	public TestData(long time, byte type, double value) {
		super();
		this.time = time;
		this.type = type;
		this.value = value;
	}

	@Override
	public long getTimestamp() {
		return time;
	}

	@Override
	public int compareTo(Temporal o) {
		return Long.valueOf(time).compareTo(o.getTimestamp());
	}

}