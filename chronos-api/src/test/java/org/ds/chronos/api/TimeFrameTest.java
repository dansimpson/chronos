package org.ds.chronos.api;

import org.ds.chronos.support.TestBase;
import org.ds.chronos.util.TimeFrame;
import org.junit.Assert;
import org.junit.Test;

public class TimeFrameTest extends TestBase {

	@Test
	public void giveDuration() throws ChronosException {
		Assert.assertEquals("100ms", new TimeFrame(0, 100).getDuration().toString());
	}
	
	@Test
	public void contains() throws ChronosException {
		TimeFrame t = new TimeFrame(10, 100);
		
		Assert.assertTrue(t.contains(55));
		Assert.assertFalse(t.contains(101));
		Assert.assertFalse(t.contains(1));
		
		
		Assert.assertFalse(t.contains(1, 20));
		Assert.assertTrue(t.intersects(1, 20));
		Assert.assertTrue(t.contains(11, 20));
		Assert.assertFalse(t.contains(90, 110));
		Assert.assertTrue(t.intersects(90, 110));
	}
	
	@Test
	public void narrowTest() throws ChronosException {
		TimeFrame t = new TimeFrame(0, 100);
		
		Assert.assertEquals(0, t.fit(-1, 101).getStart());
		Assert.assertEquals(100, t.fit(-1, 101).getEnd());
		Assert.assertEquals(10, t.fit(10, 99).getStart());
		Assert.assertEquals(99, t.fit(10, 99).getEnd());
		Assert.assertEquals(null, t.fit(101, 999));
	}

}
