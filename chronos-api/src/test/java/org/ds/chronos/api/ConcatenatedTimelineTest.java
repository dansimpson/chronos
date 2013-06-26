package org.ds.chronos.api;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.ds.chronos.api.chronicle.MemoryChronicle;
import org.ds.chronos.support.TestBase;
import org.ds.chronos.support.TestData;
import org.ds.chronos.support.TestDecoder;
import org.ds.chronos.support.TestEncoder;
import org.ds.chronos.timeline.ConcatenatedTimeline;
import org.ds.chronos.timeline.ConcatenatedTimeline.ScopedTimeline;
import org.ds.chronos.timeline.SimpleTimeline;
import org.ds.chronos.util.TimeFrame;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;

public class ConcatenatedTimelineTest extends TestBase {

	private MemoryChronicle chronicle1;
	private MemoryChronicle chronicle2;

	private Timeline<TestData> t1;
	private Timeline<TestData> t2;
	private ConcatenatedTimeline<TestData> store;

	@SuppressWarnings("unchecked")
	@Before
	public void createChronicle() {
		chronicle1 = new MemoryChronicle();
		chronicle2 = new MemoryChronicle();
		t1 = new SimpleTimeline<TestData>(chronicle1, new TestDecoder(), new TestEncoder());
		t2 = new SimpleTimeline<TestData>(chronicle2, new TestDecoder(), new TestEncoder());

		store = new ConcatenatedTimeline<TestData>(new ScopedTimeline<TestData>(t1, new TimeFrame(0, 1000)),
		    new ScopedTimeline<TestData>(t2, new TimeFrame(1001, 2000)));
	}

	@Test
	public void distributeItem() throws ChronosException {
		store.add(new TestData(100, (byte) 0, 20d));
		store.add(new TestData(1100, (byte) 0, 20d));
		Assert.assertEquals(2, store.getNumEvents(0, 5000));
		Assert.assertEquals(1, t1.getNumEvents(0, 5000));
		Assert.assertEquals(1, t2.getNumEvents(0, 5000));
	}

	@Test
	public void exists() throws ChronosException {
		store.add(new TestData(100, (byte) 0, 20d));
		Assert.assertTrue(store.isEventRecorded(100));
		Assert.assertTrue(t1.isEventRecorded(100));
		Assert.assertFalse(t2.isEventRecorded(100));
	}

	@Test
	public void remove() throws ChronosException {
		store.add(new TestData(100, (byte) 0, 20d));
		store.add(new TestData(1100, (byte) 0, 20d));
		store.deleteRange(0, 5500);
		Assert.assertEquals(0, store.getNumEvents(0, 5000));
		Assert.assertEquals(0, t1.getNumEvents(0, 5000));
		Assert.assertEquals(0, t2.getNumEvents(0, 5000));
	}

	@Test
	public void iterate() throws ChronosException {
		store.add(new TestData(100, (byte) 0, 20d));
		store.add(new TestData(1100, (byte) 0, 20d));
		Assert.assertEquals(2, Iterables.size(store.iterable(0, 2000)));
	}

	@Test
	public void addBatch() throws ChronosException {
		int count = 2000;
		List<TestData> list = new ArrayList<TestData>();
		for (int i = 0; i < count; i++) {
			TestData data = new TestData();
			data.time = i;
			data.type = 0x05;
			data.value = i <= 1000 ? 1d : 2d;
			list.add(data);
		}
		store.add(list);

		Assert.assertEquals(count, store.getNumEvents(0, 5000));
	}

}
