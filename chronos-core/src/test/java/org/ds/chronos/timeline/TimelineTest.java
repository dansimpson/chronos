package org.ds.chronos.timeline;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.ds.chronos.chronicle.MemoryChronicle;
import org.ds.support.TestData;
import org.ds.support.TestDecoder;
import org.ds.support.TestEncoder;
import org.junit.Before;
import org.junit.Test;

public class TimelineTest {

	private MemoryChronicle chronicle;
	private Timeline<TestData> store;
	
	@Before
	public void createChronicle() {
		chronicle = new MemoryChronicle();
		store = new Timeline<TestData>(chronicle, new TestDecoder(), new TestEncoder());
	}

	@Test
	public void testSingleItem() {
		TestData data = new TestData();
		data.time = 1000;
		data.type = 0x05;
		data.value = 1337.1337d;
		store.add(data);
		
		Assert.assertEquals(1, chronicle.getNumEvents(0, System.currentTimeMillis()));
		
		TestData compare = store.query(new Date(0), new Date(), TestData.class).first();
		Assert.assertEquals(data.time, compare.time);
		Assert.assertEquals(data.type, compare.type);
		Assert.assertEquals(data.value, compare.value, 0.0);
	}
	
	@Test(timeout=3000)
	public void testManyItems() {
		int count = 500000;
		List<TestData> list = new ArrayList<TestData>();
		for(int i = 0;i < count;i++) {
			TestData data = new TestData();
			data.time = i * 1000;
			data.type = 0x05;
			data.value = 1337.1337d;
			list.add(data);
		}
		store.add(list);
		
		Assert.assertEquals(count, chronicle.getNumEvents(0, System.currentTimeMillis()));
		Iterator<TestData> data = store.query(new Date(0), new Date(), TestData.class).iterator();
		
		long t1 = System.currentTimeMillis();
		count = 0;
		while(data.hasNext()) {
			Assert.assertEquals(count++ * 1000, data.next().time);
		}
		System.out.printf("%d items decoded in %d", count, System.currentTimeMillis() - t1);
	}
	
	
	@Test(timeout=3000)
	public void testQuery() {
		int count = 5000;
		List<TestData> list = new ArrayList<TestData>();
		for(int i = 0;i < count;i++) {
			TestData data = new TestData();
			data.time = i * 1000;
			data.type = 0x05;
			data.value = 1337.1337d;
			list.add(data);
		}
		store.add(list);

		Assert.assertEquals(count, store.query(0, count * 1000, TestData.class).list().size());
	}
}
