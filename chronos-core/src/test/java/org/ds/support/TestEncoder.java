package org.ds.support;

import java.nio.ByteBuffer;
import java.util.Iterator;

import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;

import org.ds.chronos.timeline.TimelineEncoder;

public class TestEncoder implements TimelineEncoder<TestData> {

	private Iterator<TestData> input;
	
	@Override
	public boolean hasNext() {
		return input.hasNext();
	}

	@Override
	public HColumn<Long, byte[]> next() {
		TestData data = input.next();
		ByteBuffer buffer = ByteBuffer.allocate(9);
		buffer.put(data.type);
		buffer.putDouble(data.value);
		buffer.rewind();
		return HFactory.createColumn(data.time, buffer.array());
	}

	@Override
	public void remove() {
	}

	@Override
	public void setInputStream(Iterator<TestData> input) {
		this.input = input;
	}
}