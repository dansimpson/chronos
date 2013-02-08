package org.ds.chronos.api;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.ds.chronos.api.chronicle.MemoryChronicle;
import org.ds.chronos.support.TestBase;
import org.junit.Test;

public class MemoryChronicleTest extends TestBase {

  protected ChronologicalRecord getTestItem(long time) {
    return new ChronologicalRecord(time, "Hello".getBytes());
  }

  protected List<ChronologicalRecord> getTestItemList(long startTime,
      long periodInMillis, int count) {
    List<ChronologicalRecord> result = new ArrayList<ChronologicalRecord>();
    for (int i = 0; i < count; i++) {
      result.add(getTestItem(startTime + (periodInMillis * i)));
    }
    return result;
  }

  @Test
  public void testAdd() {
    Chronicle chronicle = new MemoryChronicle();
    chronicle.add(getTestItem(0));
    Assert.assertEquals(1,
        chronicle.getNumEvents(0, System.currentTimeMillis()));
  }

  @Test
  public void testAddBatch() {
    Chronicle chronicle = new MemoryChronicle();
    chronicle.add(getTestItemList(0, 1000, 100));
    Assert.assertEquals(100,
        chronicle.getNumEvents(0, System.currentTimeMillis()));
  }

  @Test
  public void testRange() {
    Chronicle chronicle = new MemoryChronicle();
    chronicle.add(getTestItemList(0, 1000, 100));

    List<ChronologicalRecord> items = MemoryChronicle.toList(chronicle.getRange(1000, 5000));

    Assert.assertEquals(5, items.size());
    Assert.assertEquals(1000, items.get(0).getTimestamp());
    Assert.assertEquals(5000, items.get(4).getTimestamp());
  }

  @Test
  public void testReverseRange() {
    Chronicle chronicle = new MemoryChronicle();
    chronicle.add(getTestItemList(0, 1000, 100));
    
    List<ChronologicalRecord> items = MemoryChronicle.toList(chronicle.getRange(5000, 1000));

    Assert.assertEquals(5, items.size());
    Assert.assertEquals(5000, items.get(0).getTimestamp());
    Assert.assertEquals(1000, items.get(4).getTimestamp());
  }

  @Test
  public void testCount() {
    Chronicle chronicle = new MemoryChronicle();
    chronicle.add(getTestItemList(0, 1000, 100));
    Assert.assertEquals(50, chronicle.getNumEvents(1, 50000));
  }

}