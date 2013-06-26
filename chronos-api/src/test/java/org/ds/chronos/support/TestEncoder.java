package org.ds.chronos.support;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.ds.chronos.api.ChronologicalRecord;
import org.ds.chronos.api.TimelineEncoder;

public class TestEncoder implements TimelineEncoder<TestData> {

  private Iterator<TestData> input;

  @Override
  public boolean hasNext() {
    return input.hasNext();
  }

  @Override
  public ChronologicalRecord next() {
    TestData data = input.next();
    ByteBuffer buffer = ByteBuffer.allocate(9);
    buffer.put(data.type);
    buffer.putDouble(data.value);
    buffer.rewind();
    return new ChronologicalRecord(data.time, buffer.array());
  }

  @Override
  public void remove() {
  }

  @Override
  public void setInputStream(Iterator<TestData> input) {
    this.input = input;
  }
}