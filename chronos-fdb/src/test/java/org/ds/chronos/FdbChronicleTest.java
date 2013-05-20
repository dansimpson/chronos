package org.ds.chronos;

import java.util.Date;
import java.util.Iterator;

import junit.framework.Assert;

import org.ds.chronos.api.Chronicle;
import org.ds.chronos.api.ChronicleBatch;
import org.ds.chronos.api.ChronologicalRecord;
import org.ds.chronos.api.ChronosException;
import org.ds.chronos.api.chronicle.MemoryChronicle;
import org.ds.chronos.chronicle.FdbChronicle;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Iterators;

public class FdbChronicleTest extends TestBase {

	private Chronicle chronicle;

	@Before
	public void create() throws ChronosException {
		chronicle = new FdbChronicle(getDatabase(), "chronos-test");
		chronicle.delete();
	}

	@After
	public void destroy() throws ChronosException {
		chronicle.delete();
	}

	@Test
	public void shouldBeEmpty() throws ChronosException {
		Assert.assertEquals(0, chronicle.getNumEvents(0, System.currentTimeMillis()));
	}

	@Test
	public void shouldAddEventAndCount() throws ChronosException {
		chronicle.add(1000, "test");
		Assert.assertEquals(1, chronicle.getNumEvents(999, 1001));
	}

	@Test
	public void shouldBatch() throws ChronosException {
		long count = 100;
		long period = 1000 * 60;
		long time = 0;

		ChronicleBatch batch = new ChronicleBatch();
		for (int i = 0; i < count; i++) {
			batch.add(time + i * period, "test");
		}
		chronicle.add(batch);

		Assert.assertEquals(count, chronicle.getNumEvents(0, count * period));
	}

	@Test
	public void shouldGetSingleRange() throws ChronosException {
		chronicle.add(1000, "test");
		Iterator<ChronologicalRecord> slice = chronicle.getRange(0, 2000);

		Assert.assertTrue(slice.hasNext());
		ChronologicalRecord event = slice.next();
		Assert.assertEquals(1000, event.getTimestamp());
		Assert.assertEquals("test", new String(event.getData(), Charsets.UTF_8));
	}

	@Test
	public void shouldGetRange() throws ChronosException {
		chronicle.add(1000, "test");
		chronicle.add(2000, "test");
		Iterator<ChronologicalRecord> slice = chronicle.getRange(1000, 3000);

		Assert.assertTrue(slice.hasNext());
		ChronologicalRecord event = slice.next();

		Assert.assertEquals(1000, event.getTimestamp());
		Assert.assertEquals("test", new String(event.getData(), Charsets.UTF_8));
	}

	@Test
	public void shouldGetRangeReversed() throws ChronosException {
		chronicle.add(1000, "test");
		chronicle.add(2000, "test");
		Iterator<ChronologicalRecord> slice = chronicle.getRange(3000, 1000);

		Assert.assertTrue(slice.hasNext());
		ChronologicalRecord event = slice.next();

		Assert.assertEquals(2000, event.getTimestamp());
		Assert.assertEquals("test", new String(event.getData(), Charsets.UTF_8));
	}

	@Test
	public void shouldDelete() throws ChronosException {
		Date time = new Date();

		chronicle.add(time, "test");
		chronicle.deleteRange(time.getTime() - 1000, time.getTime() + 1000);

		Assert.assertEquals(0, chronicle.getNumEvents(0, System.currentTimeMillis()));
	}

	@Test
	public void shouldDeleteFragment() throws ChronosException {
		chronicle.add(1000, "test");
		chronicle.add(2000, "test");
		chronicle.add(3000, "test");
		chronicle.add(4000, "test");

		chronicle.deleteRange(0, 2000);

		Assert.assertEquals(2, chronicle.getNumEvents(0, 4000));
		Assert.assertEquals(3000, MemoryChronicle.toList(chronicle.getRange(0, 4000)).get(0).getTimestamp());
	}

	@Test
	public void shouldDeleteRow() throws ChronosException {
		chronicle.add(1000, "test");
		chronicle.delete();

		Assert.assertEquals(0, chronicle.getNumEvents(0, 2000));
	}

	@Test
	public void shouldDetectExistence() throws ChronosException {
		chronicle.add(1000, "test");
		Assert.assertTrue(chronicle.isEventRecorded(1000));
		Assert.assertFalse(chronicle.isEventRecorded(999));
		Assert.assertFalse(chronicle.isEventRecorded(1001));
	}

	@Test(timeout = 5000)
	public void testPerf() {
		long count = 2800;
		long period = 1000 * 60;
		long time = System.currentTimeMillis();

		ChronicleBatch batch = new ChronicleBatch();
		for (int i = 0; i < count; i++) {
			batch.add(time + i * period, "test");
		}
		long t1 = System.currentTimeMillis();
		chronicle.add(batch);
		long t2 = System.currentTimeMillis();
		System.out.printf("%d ms for %d items\n", t2 - t1, count);

		t1 = System.currentTimeMillis();
		chronicle.add(new ChronologicalRecord(time - period, "test".getBytes()));
		t2 = System.currentTimeMillis();

		System.out.printf("%d ms for appending one more\n", t2 - t1);

		int numRead = 0;

		t1 = System.currentTimeMillis();
		for (int i = 0; i < 100; i++) {
			long start = time;
			long end = time + Math.round(Math.random() * (count * period));
			numRead += Iterators.toArray(chronicle.getRange(start, end), ChronologicalRecord.class).length;
		}
		t2 = System.currentTimeMillis();

		System.out.printf("%d ms for ranging 100 times, reading %d samples\n", t2 - t1, numRead);
	}

}