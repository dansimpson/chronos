package org.ds.chronos;

import java.util.Date;
import java.util.Iterator;

import junit.framework.Assert;
import me.prettyprint.hector.api.beans.HColumn;

import org.ds.chronos.api.Chronicle;
import org.ds.chronos.api.ChronicleBatch;
import org.ds.chronos.api.Chronos;
import org.ds.chronos.api.ChronosException;
import org.ds.support.TestBaseWithCassandra;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;

public class CassandraChonicleTest extends TestBaseWithCassandra {
    
	private static int count = 1;
	private Chronicle chronicle;
	
	@Before
	public void create() throws ChronosException {
		chronicle = getChronos("test").getChronicle("basicTest" + count++);
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
		for(int i = 0;i < count;i++) {
			batch.add(time + i * period, "test");
		}
		chronicle.add(batch);

		Assert.assertEquals(count, chronicle.getNumEvents(0, count * period));
	}
	
	@Test
	public void shouldGetSingleRange() throws ChronosException {
		chronicle.add(1000, "test");
		Iterator<HColumn<Long, byte[]>> slice = chronicle.getRange(0, 2000);

		Assert.assertTrue(slice.hasNext());
		HColumn<Long, byte[]> event = slice.next();
		Assert.assertEquals(1000, event.getName().longValue());
		Assert.assertEquals("test", new String(event.getValue(), Charsets.UTF_8));
	}
	
	@Test
	public void shouldGetRange() throws ChronosException {
		chronicle.add(1000, "test");
		chronicle.add(2000, "test");
		Iterator<HColumn<Long, byte[]>> slice = chronicle.getRange(1000, 3000);

		Assert.assertTrue(slice.hasNext());
		HColumn<Long, byte[]> event = slice.next();
		
		Assert.assertEquals(1000, event.getName().longValue());
		Assert.assertEquals("test", new String(event.getValue(), Charsets.UTF_8));
	}
	
	@Test
	public void shouldGetRangeReversed() throws ChronosException {
		chronicle.add(1000, "test");
		chronicle.add(2000, "test");
		Iterator<HColumn<Long, byte[]>> slice = chronicle.getRange(3000, 1000);

		Assert.assertTrue(slice.hasNext());
		HColumn<Long, byte[]> event = slice.next();
		
		Assert.assertEquals(2000, event.getName().longValue());
		Assert.assertEquals("test", new String(event.getValue(), Charsets.UTF_8));
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
		Assert.assertEquals(3000, Chronos.toList(chronicle.getRange(0, 4000)).get(0).getName().longValue());
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
	
}
