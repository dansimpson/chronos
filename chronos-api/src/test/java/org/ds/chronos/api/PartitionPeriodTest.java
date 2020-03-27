package org.ds.chronos.api;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.ds.chronos.api.chronicle.MemoryChronicle;
import org.ds.chronos.support.TestBase;
import org.ds.chronos.util.Duration;
import org.junit.Test;

import com.google.common.collect.Iterators;

public class PartitionPeriodTest extends TestBase {

	@Test
	public void shouldGenerateHourKey() throws ChronosException {
		Assert.assertEquals("key-2018-10-08-18", PartitionPeriod.HOUR.getPeriodKey("key", 1539023421354l));
	}

	@Test
	public void shouldGenerateRowKeysForHours() throws ChronosException {
		PartitionPeriod period = PartitionPeriod.HOUR;

		Calendar calendar = Calendar.getInstance();
		calendar.set(2012, 0, 1, 0, 0, 0);
		Date d1 = calendar.getTime();
		calendar.set(2012, 0, 2, 23, 59, 59);
		Date d2 = calendar.getTime();

		List<String> keys = period.getPeriodKeys("key", d1, d2);
		Assert.assertEquals("key-2012-01-01-00", keys.get(0));
		Assert.assertEquals("key-2012-01-02-23", keys.get(keys.size() - 1));
	}

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

	@Test
	public void shouldIncludeBothYears() throws ChronosException {
		PartitionPeriod period = PartitionPeriod.YEAR;

		Calendar calendar = Calendar.getInstance();
		calendar.set(2012, 11, 29, 0, 0, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		Date d1 = calendar.getTime();
		calendar.add(Calendar.DATE, 15);
		Date d2 = calendar.getTime();
		List<String> keys = period.getPeriodKeys("key", d1, d2);

		Assert.assertEquals(2, keys.size());
		Assert.assertEquals("key-2012", keys.get(0));
		Assert.assertEquals("key-2013", keys.get(1));
	}

	@Test
	public void shouldInclude3Years() throws ChronosException {
		PartitionPeriod period = PartitionPeriod.YEAR;

		Calendar calendar = Calendar.getInstance();
		calendar.set(2012, 11, 29, 0, 0, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		Date d1 = calendar.getTime();
		calendar.add(Calendar.DATE, 15 + 365);
		Date d2 = calendar.getTime();
		List<String> keys = period.getPeriodKeys("key", d1, d2);

		Assert.assertEquals(3, keys.size());
		Assert.assertEquals("key-2012", keys.get(0));
		Assert.assertEquals("key-2013", keys.get(1));
		Assert.assertEquals("key-2014", keys.get(2));
	}

	@Test
	public void shouldInclude3Months() throws ChronosException {
		PartitionPeriod period = PartitionPeriod.MONTH;

		Calendar calendar = Calendar.getInstance();
		calendar.set(2012, 11, 29, 0, 0, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		Date d1 = calendar.getTime();
		calendar.add(Calendar.DATE, 45);
		Date d2 = calendar.getTime();
		List<String> keys = period.getPeriodKeys("key", d1, d2);

		Assert.assertEquals(3, keys.size());
		Assert.assertEquals("key-2012-12", keys.get(0));
		Assert.assertEquals("key-2013-01", keys.get(1));
		Assert.assertEquals("key-2013-02", keys.get(2));
	}

	@Test
	public void shouldHitProperChronicle() throws ChronosException {
		PartitionPeriod period = PartitionPeriod.YEAR;

		Calendar calendar = Calendar.getInstance();
		calendar.set(2012, 11, 29, 0, 0, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		long start = calendar.getTimeInMillis();
		calendar.add(Calendar.DATE, 15);
		long end = calendar.getTimeInMillis();

		TestChronicle chron = new TestChronicle("test", period);

		long time = start;
		while (time < end) {
			chron.add(new ChronologicalRecord(time, "x".getBytes()));
			time += new Duration("1m").getMillis();
		}

		Assert.assertEquals(2, chron.chronicles.size());
		Assert.assertNotNull(chron.chronicles.get("test-2012"));
		Assert.assertNotNull(chron.chronicles.get("test-2013"));
		Assert.assertEquals(1440 * 3, chron.chronicles.get("test-2012").size());
		Assert.assertEquals(1440 * 12, chron.chronicles.get("test-2013").size());
		Assert.assertEquals(1440 * 3, chron.chronicles.get("test-2012").getNumEvents(start, end));
		Assert.assertEquals(1440 * 12, chron.chronicles.get("test-2013").getNumEvents(start, end));
		Assert.assertEquals(1440 * 15, chron.getNumEvents(start, end));
		Assert.assertEquals(1440 * 15, Iterators.size(chron.getRange(start, end)));
	}

	private static class TestChronicle extends PartitionedChronicle {

		public final Map<String, MemoryChronicle> chronicles;

		public TestChronicle(String keyPrefix, PartitionPeriod period) {
			super(keyPrefix, period);
			chronicles = new HashMap<String, MemoryChronicle>();
		}

		@Override
		public Chronicle getPartition(String key) {
			if (!chronicles.containsKey(key)) {
				chronicles.put(key, new MemoryChronicle());
			}
			return chronicles.get(key);
		}

	}
}
