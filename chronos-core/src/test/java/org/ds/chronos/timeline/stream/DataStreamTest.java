package org.ds.chronos.timeline.stream;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.ds.chronos.timeline.stream.DataStreamAggregator.Aggregator;
import org.ds.chronos.timeline.stream.DataStreamFilter.FilterFn;
import org.ds.chronos.timeline.stream.DataStreamMap.MapFn;
import org.ds.chronos.timeline.stream.DataStreamTransform.TransformFn;
import org.ds.support.TestData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DataStreamTest {

	public static FilterFn<TestData> range(final double low, final double high) {
		return new FilterFn<TestData>() {
			public boolean check(TestData item) {
				return item.value >= low && item.value < high;
			}
		};
	}
	
	public static FilterFn<Double> gte(final double v) {
		return new FilterFn<Double>() {
			public boolean check(Double item) {
				return item >= v;
			}
		};
	}
	
	public static MapFn<TestData, Double> doublize() {
		return new MapFn<TestData, Double>() {
			public Double map(TestData item) {
				return item.value;
			}
		};
	}
	
	public static TransformFn<Double> multiply(final double factor) {
		return new TransformFn<Double>() {
			public Double map(Double d) {
				return d * factor;
			}
		};
	}
	
	public static Aggregator<Double, Double> avg(final int window) {
		return new Aggregator<Double, Double>() {

			int count = 0;
			double sum = 0d;
			
			public boolean feed(Double item) {
				sum += item;
				if(++count == window) {
					return true;
				}
				return false;
			}

			public Double flush() {
				if(count == 0) {
					return null;
				}
				
				double result = sum / count;
				sum = 0d;
				count = 0;
				return result;
			}
			
		};
	}
	
	private List<TestData> items;
	
	private static final int STORE_SIZE = 1000;
	
	private ArrayList<TestData> getData(int count) {
		ArrayList<TestData> items = new ArrayList<TestData>();
		for(int i = 0;i < count;i++) {
			TestData data = new TestData();
			data.time = 1000 * i;
			data.type = 0x05;
			data.value = i;
			items.add(data);
		}
		return items;
	}
	
	@Before
	public void createChronicle() {
		items = getData(STORE_SIZE);
	}
	
	@Test
	public void testPipeline() {
		DataStream<TestData> pipeline = new DataStream<TestData>(items.iterator());
		int count = 0;
		while(pipeline.iterator().hasNext()) {
			pipeline.iterator().next();
			count++;
		}
		Assert.assertEquals(STORE_SIZE, count);
	}
	
	@Test
	public void testResultSet() {
		List<TestData> result = new DataStream<TestData>(items.iterator()).list();
		Assert.assertEquals(STORE_SIZE, result.size());
	}
	
	@Test
	@SuppressWarnings("unused")
	public void testStream() {
		int count = 0;
		for(TestData data: new DataStream<TestData>(items.iterator()).stream()) {
			count++;
		}
		Assert.assertEquals(STORE_SIZE, count);
	}
	
	@Test
	public void testPipelineFilter() {
		List<TestData> result = new DataStream<TestData>(items.iterator()).filter(range(100, 200)).list();
		Assert.assertEquals(200 - 100, result.size());
	}
	
	@Test
	public void testMultifilter() {
		List<TestData> result = new DataStream<TestData>(items.iterator())
			.filter(range(100, 200))
			.filter(range(110, 120))
			.list();
		
		Assert.assertEquals(120 - 110, result.size());
	}
	
	@Test
	public void testMap() {
		List<Double> result = new DataStream<Double>(items.iterator())
			.map(doublize())
			.list();
		Assert.assertEquals(STORE_SIZE, result.size());
	}
	
	@Test
	public void testTransform() {
		List<Double> result = new DataStream<Double>(items.iterator())
			.map(doublize())
			.filter(gte(5))
			.transform(multiply(-1d))
			.transform(multiply(2d))
			.list();
		
		Assert.assertEquals(STORE_SIZE - 5, result.size());
		Assert.assertEquals(-10d, result.get(0), 0.1);
	}
	
	@Test(timeout=2000)
	public void testAggregate() {
		int inputSize = 60 * 60 * 24;
		List<Double> result = new DataStream<Double>(getData(inputSize).iterator())
			.map(doublize())
			.aggregate(avg(1800))
			.list();
		
		Assert.assertEquals(48, result.size());
	}
	
//	@Test // Takes a white with 10BB items
//	10000000000 items ready 11 MB
//	10000000000 items staged 11 MB
//	10000000000 items processed 29 MB 295860 ms
	public void testMemory() {
		Runtime r = Runtime.getRuntime();

		// increase to 10B
		final long n = 10000000000l;
		
		final TestData data = new TestData();
		data.time = 1000;
		data.type = 0x05;
		data.value = 50;
		
		Iterator<TestData> iterator = new Iterator<TestData>() {
			long x = 0;

			public boolean hasNext() {
				return x++ < n;
			}

			public TestData next() {
				return data;
			}

			public void remove() {
			}
		};
		
		System.out.printf("%d items ready %d MB\n", n, (r.totalMemory() - r.freeMemory()) / (1024 * 1024));
		
		long t1 = System.currentTimeMillis();
		Iterator<Double> result = new DataStream<Double>(iterator)
			.map(doublize())
			.filter(gte(5))
			.transform(multiply(-1d))
			.transform(multiply(2d))
			.aggregate(avg(10000))
			.iterator();
		
		System.out.printf("%d items staged %d MB\n", n, (r.totalMemory() - r.freeMemory()) / (1024 * 1024));
		
		while(result.hasNext()) {
			result.next();
		}
		
		System.out.printf("%d items processed %d MB %d ms\n", n, (r.totalMemory() - r.freeMemory()) / (1024 * 1024), System.currentTimeMillis() - t1);
	}
}
