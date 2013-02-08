package org.ds.chronos;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import junit.framework.Assert;

import org.ds.chronos.api.ChronicleBatch;
import org.ds.chronos.api.ChronologicalRecord;
import org.ds.chronos.api.ChronosException;
import org.ds.chronos.api.chronicle.MemoryChronicle;
import org.ds.chronos.chronicle.PartitionPeriod;
import org.ds.chronos.chronicle.PartitionedChronicle;
import org.ds.support.TestBaseWithCassandra;
import org.junit.Test;

public class ParitionedChronicleTest extends TestBaseWithCassandra {

  private PartitionedChronicle getChronicle(String prefix,
      PartitionPeriod period) throws ChronosException {
    return (PartitionedChronicle) getChronos("test").getChronicle(prefix,
        period);
  }

  @Test
  public void shouldPartitionData() throws ChronosException {
    PartitionedChronicle chronicle = getChronicle("day-allocation",
        PartitionPeriod.DAY);

    Calendar calendar = Calendar.getInstance();
    calendar.set(2012, 0, 1);

    Date d1 = calendar.getTime();

    for (int i = 0; i < 5; i++) {
      chronicle.add(calendar.getTime(), "test");
      calendar.add(Calendar.DAY_OF_YEAR, 1);
    }
    calendar.add(Calendar.DAY_OF_YEAR, -1);

    Date d2 = calendar.getTime();

    List<ChronologicalRecord> items = MemoryChronicle.toList(chronicle.getRange(d1,
        d2));

    Assert.assertEquals(5, items.size());
    Assert.assertEquals(5, chronicle.getNumEvents(d1, d2));
  }

  @Test(timeout = 3000)
  public void shouldBatch() throws ChronosException {
    PartitionedChronicle chronicle = getChronicle("day-batch",
        PartitionPeriod.DAY);

    Calendar calendar = Calendar.getInstance();
    calendar.set(2012, 0, 1);

    Date d1 = calendar.getTime();

    int numSamples = 1440 * 7;

    ChronicleBatch batch = new ChronicleBatch();
    for (int i = 0; i < numSamples; i++) {
      batch.add(calendar.getTime(), "test");
      calendar.add(Calendar.MINUTE, 1);
    }

    chronicle.add(batch);

    List<ChronologicalRecord> events = MemoryChronicle.toList(chronicle.getRange(d1,
        calendar.getTime()));

    Assert.assertEquals(numSamples, events.size());
    Assert.assertEquals(d1.getTime(), events.get(0).getTimestamp());
    Assert.assertEquals(numSamples,
        chronicle.getNumEvents(d1, calendar.getTime()));
  }

  @Test
  public void testReverseRange() throws ChronosException {
    PartitionedChronicle chronicle = getChronicle("day-batch-rev",
        PartitionPeriod.DAY);

    Calendar calendar = Calendar.getInstance();
    calendar.set(2012, 0, 1);

    Date d1 = calendar.getTime();

    int numSamples = 1440 * 7;

    ChronicleBatch batch = new ChronicleBatch();
    for (int i = 0; i < numSamples; i++) {
      batch.add(calendar.getTime(), "test");
      calendar.add(Calendar.MINUTE, 1);
    }
    calendar.add(Calendar.MINUTE, -1);

    chronicle.add(batch);

    List<ChronologicalRecord> events = MemoryChronicle.toList(chronicle.getRange(
        calendar.getTime(), d1));

    Assert.assertEquals(numSamples, events.size());
    Assert.assertEquals(calendar.getTime(), new Date(events.get(0).getTimestamp()));
  }

  @Test(timeout = 15000)
  public void shouldBatchMonthQuickly() throws ChronosException {
    PartitionedChronicle chronicle = getChronicle("month-batch",
        PartitionPeriod.MONTH);

    Calendar calendar = Calendar.getInstance();
    calendar.set(2012, 0, 1);

    Date d1 = calendar.getTime();

    int numSamples = 1440 * 60;

    ChronicleBatch batch = new ChronicleBatch();
    for (int i = 0; i < numSamples; i++) {
      batch.add(calendar.getTime(), "test");
      calendar.add(Calendar.MINUTE, 1);
    }

    Date d2 = calendar.getTime();

    chronicle.add(batch);

    List<ChronologicalRecord> events = MemoryChronicle.toList(chronicle.getRange(d1,
        d2));
    Assert.assertEquals(numSamples, events.size());
    Assert.assertEquals(d1.getTime(), events.get(0).getTimestamp());
    Assert.assertEquals(numSamples,
        chronicle.getNumEvents(d1, calendar.getTime()));
  }

  @Test(timeout = 15000)
  public void shouldDeleteRange() throws ChronosException {
    PartitionedChronicle chronicle = getChronicle("day-batch-rm",
        PartitionPeriod.DAY);

    Calendar calendar = Calendar.getInstance();
    calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
    calendar.set(Calendar.MILLISECOND, 0);
    calendar.set(2012, 0, 1, 21, 0, 0);
    Date r1 = calendar.getTime();
    calendar.set(2012, 0, 2, 1, 0, 0);
    Date r2 = calendar.getTime();
    calendar.set(2012, 0, 1, 0, 0, 0);
    Date d1 = calendar.getTime();

    int numSamples = 1440 * 2;
    ChronicleBatch batch = new ChronicleBatch();
    for (int i = 0; i < numSamples; i++) {
      batch.add(calendar.getTime(), "test");
      calendar.add(Calendar.MINUTE, 1);
    }
    calendar.add(Calendar.MINUTE, -1);
    Date d2 = calendar.getTime();

    chronicle.add(batch);
    chronicle.deleteRange(r1, r2);

    List<ChronologicalRecord> events = MemoryChronicle.toList(chronicle.getRange(d1,
        d2));

    Assert.assertEquals(numSamples - ((4 * 60) + 1), events.size());
    Assert.assertEquals(numSamples - ((4 * 60) + 1),
        chronicle.getNumEvents(d1, d2));
    Assert.assertEquals(d1.getTime(), events.get(0).getTimestamp());
  }

  @Test
  public void shouldDetectColumn() throws ChronosException {
    PartitionedChronicle chronicle = getChronicle("day-existence-test",
        PartitionPeriod.DAY);
    Calendar calendar = Calendar.getInstance();
    calendar.set(2012, 0, 1);
    chronicle.add(calendar.getTime(), "whoa");
    Assert.assertTrue(chronicle.isEventRecorded(calendar.getTimeInMillis()));
  }

}
