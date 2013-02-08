package org.ds.chronos.support;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.ds.chronos.api.ChronologicalRecord;
import org.ds.chronos.timeline.TimelineDecoder;

public class TestDecoder implements TimelineDecoder<TestData> {

  private Iterator<ChronologicalRecord> input;

  @Override
  public void setInputStream(Iterator<ChronologicalRecord> input) {
    this.input = input;
  }

  @Override
  public boolean hasNext() {
    return input.hasNext();
  }

  @Override
  public TestData next() {
    ChronologicalRecord column = input.next();
    ByteBuffer buffer = column.getValueBytes();

    TestData data = new TestData();
    data.time = column.getTimestamp();
    data.type = buffer.get();
    data.value = buffer.getDouble();
    return data;
  }

  @Override
  public void remove() {
  }

}