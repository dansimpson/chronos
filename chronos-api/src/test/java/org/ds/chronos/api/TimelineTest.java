package org.ds.chronos.api;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import junit.framework.Assert;

import org.ds.chronos.api.chronicle.MemoryChronicle;
import org.ds.chronos.support.TestData;
import org.ds.chronos.support.TestDecoder;
import org.ds.chronos.support.TestEncoder;
import org.ds.chronos.timeline.Timeline;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;

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

		Optional<TestData> option = store.query(new Date(0), new Date()).first();
		Assert.assertTrue(option.isPresent());

		TestData compare = option.get();
		Assert.assertEquals(data.time, compare.time);
		Assert.assertEquals(data.type, compare.type);
		Assert.assertEquals(data.value, compare.value, 0.0);
	}

	@Test(timeout = 3000)
	public void testManyItems() {
		int count = 500000;
		List<TestData> list = new ArrayList<TestData>();
		for (int i = 0; i < count; i++) {
			TestData data = new TestData();
			data.time = i * 1000;
			data.type = 0x05;
			data.value = 1337.1337d;
			list.add(data);
		}
		store.add(list);

		Assert.assertEquals(count, chronicle.getNumEvents(0, System.currentTimeMillis()));
		Iterable<TestData> data = store.query(new Date(0), new Date()).stream();

		long t1 = System.currentTimeMillis();
		count = 0;
		for (TestData item : data) {
			Assert.assertEquals(count++ * 1000, item.time);
		}
		System.out.printf("%d items decoded in %d", count, System.currentTimeMillis() - t1);
	}

	@Test(timeout = 3000)
	public void testQuery() {
		int count = 5000;
		List<TestData> list = new ArrayList<TestData>();
		for (int i = 0; i < count; i++) {
			TestData data = new TestData();
			data.time = i * 1000;
			data.type = 0x05;
			data.value = 1337.1337d;
			list.add(data);
		}
		store.add(list);

		Assert.assertEquals(count, store.query(0, count * 1000).toList().size());
	}

	@Test(timeout = 3000)
	public void testFluent() {
		int count = 5000;
		List<TestData> list = new ArrayList<TestData>();
		for (int i = 0; i < count; i++) {
			TestData data = new TestData();
			data.time = i * 1000;
			data.type = 0x05;
			data.value = i;
			list.add(data);
		}
		store.add(list);

		Optional<Double> value = store.fluid(0, count * 1000).transform(new Function<TestData, Double>() {

			public Double apply(TestData test) {
				return test.value;
			}
		}).firstMatch(new Predicate<Double>() {

			public boolean apply(Double value) {
				return value > 1000;
			}
		});

		Assert.assertTrue(value.isPresent());
		Assert.assertEquals(1001, value.get(), 0.0);

	}

}
