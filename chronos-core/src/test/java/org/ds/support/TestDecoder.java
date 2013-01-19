package org.ds.support;

import java.nio.ByteBuffer;
import java.util.Iterator;

import me.prettyprint.hector.api.beans.HColumn;

import org.ds.chronos.timeline.TimelineDecoder;

public class TestDecoder implements TimelineDecoder<TestData> {

	private Iterator<HColumn<Long, byte[]>> input;
	
	@Override
	public void setInputStream(Iterator<HColumn<Long, byte[]>> input) {
		this.input = input;
	}

	@Override
	public boolean hasNext() {
		return input.hasNext();
	}

	@Override
	public TestData next() {
		HColumn<Long, byte[]> column = input.next();			
		ByteBuffer buffer = column.getValueBytes();
		
		TestData data = new TestData();
		data.time = column.getName();
		data.type = buffer.get();
		data.value = buffer.getDouble();
		return data;
	}

	@Override
	public void remove() {
	}

}