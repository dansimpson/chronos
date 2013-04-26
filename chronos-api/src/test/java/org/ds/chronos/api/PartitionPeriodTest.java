package org.ds.chronos.api;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import junit.framework.Assert;

import org.ds.chronos.support.TestBase;
import org.junit.Test;

public class PartitionPeriodTest extends TestBase {

  @Test
  public void shouldGenerateRowKeysForDays() throws ChronosException {
    PartitionPeriod period = PartitionPeriod.DAY;

    Calendar calendar = Calendar.getInstance();
    calendar.set(2012, 0, 1);
    Date d1 = calendar.getTime();
    calendar.set(2012, 8, 1);
    Date d2 = calendar.getTime();

    List<String> keys = period.getPeriodKeys("key", d1, d2);

    Assert.assertEquals(245, keys.size());
    Assert.assertEquals("key-2012-01-01", keys.get(0));
    Assert.assertEquals("key-2012-09-01", keys.get(keys.size() - 1));
  }

  @Test
  public void shouldWorkOnTightBoundaries() throws ChronosException {
    PartitionPeriod period = PartitionPeriod.DAY;

    Calendar calendar = Calendar.getInstance();
    calendar.set(2012, 0, 1, 23, 0, 0);
    Date d1 = calendar.getTime();
    calendar.set(2012, 0, 2, 1, 0, 0);
    Date d2 = calendar.getTime();

    List<String> keys = period.getPeriodKeys("key", d1, d2);

    Assert.assertEquals(2, keys.size());
    Assert.assertEquals("key-2012-01-01", keys.get(0));
    Assert.assertEquals("key-2012-01-02", keys.get(keys.size() - 1));
  }

  @Test
  public void shouldWorkInReverse() throws ChronosException {
    PartitionPeriod period = PartitionPeriod.DAY;

    Calendar calendar = Calendar.getInstance();
    calendar.set(2012, 0, 1, 23, 0, 0);
    Date d1 = calendar.getTime();
    calendar.set(2012, 0, 2, 1, 0, 0);
    Date d2 = calendar.getTime();

    List<String> keys = period.getPeriodKeys("key", d2, d1);

    Assert.assertEquals(2, keys.size());
    Assert.assertEquals("key-2012-01-02", keys.get(0));
    Assert.assertEquals("key-2012-01-01", keys.get(keys.size() - 1));
  }

  @Test
  public void shouldGenerateRowKeysForMonths() throws ChronosException {
    PartitionPeriod period = PartitionPeriod.MONTH;

    Calendar calendar = Calendar.getInstance();
    calendar.set(2012, 0, 1);
    Date d1 = calendar.getTime();
    calendar.set(2012, 8, 1);
    Date d2 = calendar.getTime();

    List<String> keys = period.getPeriodKeys("key", d1, d2);

    Assert.assertEquals(9, keys.size());
    Assert.assertEquals("key-2012-01", keys.get(0));
    Assert.assertEquals("key-2012-09", keys.get(keys.size() - 1));
  }

  @Test
  public void shouldGenerateRowKeysForYears() throws ChronosException {
    PartitionPeriod period = PartitionPeriod.YEAR;

    Calendar calendar = Calendar.getInstance();
    calendar.set(2012, 0, 1);
    Date d1 = calendar.getTime();
    calendar.set(2013, 8, 1);
    Date d2 = calendar.getTime();

    List<String> keys = period.getPeriodKeys("key", d1, d2);

    Assert.assertEquals(2, keys.size());
    Assert.assertEquals("key-2012", keys.get(0));
    Assert.assertEquals("key-2013", keys.get(1));
  }

  @Test
  public void shouldGenerateRowKeyForYear() throws ChronosException {
    PartitionPeriod period = PartitionPeriod.YEAR;

    Calendar calendar = Calendar.getInstance();
    calendar.set(2012, 0, 1);
    Date d1 = calendar.getTime();
    calendar.set(2012, 8, 1);
    Date d2 = calendar.getTime();
    List<String> keys = period.getPeriodKeys("key", d1, d2);

    Assert.assertEquals(1, keys.size());
    Assert.assertEquals("key-2012", keys.get(0));
  }

}
