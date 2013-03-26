package org.ds.chronos.api;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.ds.chronos.support.TestData;
import org.ds.chronos.timeline.stream.DataStream;
import org.ds.chronos.timeline.stream.partitioned.PartitionedDataStream;
import org.ds.chronos.timeline.stream.partitioned.TimeRangePredicate;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;

public class DataStreamTest {

	public static Predicate<TestData> range(final double low, final double high) {
		return new Predicate<TestData>() {

			public boolean apply(TestData item) {
				return item.value >= low && item.value < high;
			}
		};
	}

	public static Function<Iterable<TestData>, Double> sum() {
		return new Function<Iterable<TestData>, Double>() {

			public Double apply(Iterable<TestData> items) {
				double sum = 0d;
				for (TestData data : items) {
					sum += data.value;
				}
				return sum;
			}
		};
	}

	public static Predicate<Double> gte(final double v) {
		return new Predicate<Double>() {

			public boolean apply(Double item) {
				return item >= v;
			}
		};
	}

	public static Function<TestData, Double> doublize() {
		return new Function<TestData, Double>() {

			public Double apply(TestData item) {
				return item.value;
			}
		};
	}

	public static Function<Double, Double> multiply(final double factor) {
		return new Function<Double, Double>() {

			public Double apply(Double d) {
				return d * factor;
			}
		};
	}

	private List<TestData> items;

	private static final int STORE_SIZE = 1000;

	private ArrayList<TestData> getData(int count) {
		ArrayList<TestData> items = new ArrayList<TestData>();
		for (int i = 0; i < count; i++) {
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
		DataStream<TestData> pipeline = new DataStream<TestData>(items);
		int count = 0;
		for (@SuppressWarnings("unused")
		TestData data : pipeline.stream()) {
			count++;
		}
		Assert.assertEquals(STORE_SIZE, count);
		Assert.assertEquals(STORE_SIZE, pipeline.size());
	}

	@Test
	public void testResultSet() {
		List<TestData> result = new DataStream<TestData>(items).toList();
		Assert.assertEquals(STORE_SIZE, result.size());
	}

	@Test
	@SuppressWarnings("unused")
	public void testStream() {
		int count = 0;
		for (TestData data : new DataStream<TestData>(items).stream()) {
			count++;
		}
		Assert.assertEquals(STORE_SIZE, count);
	}

	@Test
	public void testPipelineFilter() {
		List<TestData> result = new DataStream<TestData>(items).filter(range(100, 200)).toList();
		Assert.assertEquals(200 - 100, result.size());
	}

	@Test
	public void testMultifilter() {
		List<TestData> result = new DataStream<TestData>(items).filter(range(100, 200)).filter(range(110, 120)).toList();

		Assert.assertEquals(120 - 110, result.size());
	}

	@Test
	public void testMap() {
		List<Double> result = new DataStream<TestData>(items).map(doublize()).toList();
		Assert.assertEquals(STORE_SIZE, result.size());
	}

	@Test
	public void testReduce() {
		Double result = new DataStream<TestData>(items).reduce(sum());
		Assert.assertEquals(linearSum(items.size()), result, 0.0);
	}

	@Test
	public void testPartition() {
		PartitionedDataStream<TestData> result = new DataStream<TestData>(items).partition(20);
		Assert.assertEquals(STORE_SIZE / 20, result.size());
	}

	@Test
	public void testTimePartition() {
		PartitionedDataStream<TestData> result = new DataStream<TestData>(items)
		    .partition(new TimeRangePredicate<TestData>("44s"));
		Assert.assertEquals(Math.ceil(STORE_SIZE / 44.0), result.size(), 0.0);
	}

	@Test
	public void testParitionMapReduce() {
		DataStream<Double> result = new DataStream<TestData>(items).partition(20).mapReduce(sum());

		Optional<Double> first = result.first();
		Assert.assertTrue(first.isPresent());
		Assert.assertEquals(linearSum(20), first.get(), 0.0);
	}

	@Test
	public void testTransform() {
		List<Double> result = new DataStream<TestData>(items).map(doublize()).filter(gte(5)).map(multiply(-1d))
		    .map(multiply(2d)).toList();

		Assert.assertEquals(STORE_SIZE - 5, result.size());
		Assert.assertEquals(-10d, result.get(0), 0.1);
	}

	/**
	 * @Test
	 * 
	 *       10BB items ready 11 MB, 10BB items staged 11 MB, 10BB items processed 29 MB ~5mins
	 */
	public void testMemory() {
		Runtime r = Runtime.getRuntime();

		// increase to 10B
		final long n = 10000000000l;

		final TestData data = new TestData();
		data.time = 1000;
		data.type = 0x05;
		data.value = 50;

		Iterable<TestData> itr = new Iterable<TestData>() {

			@Override
			public Iterator<TestData> iterator() {
				return new Iterator<TestData>() {

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
			}

		};

		System.out.printf("%d items ready %d MB\n", n, (r.totalMemory() - r.freeMemory()) / (1024 * 1024));

		long t1 = System.currentTimeMillis();
		Iterator<Double> result = new DataStream<TestData>(itr).map(doublize()).filter(gte(5)).map(multiply(-1d))
		    .map(multiply(2d)).stream().iterator();

		System.out.printf("%d items staged %d MB\n", n, (r.totalMemory() - r.freeMemory()) / (1024 * 1024));

		while (result.hasNext()) {
			result.next();
		}

		System.out.printf("%d items processed %d MB %d ms\n", n, (r.totalMemory() - r.freeMemory()) / (1024 * 1024),
		    System.currentTimeMillis() - t1);
	}

	private static double linearSum(int count) {
		double result = 0d;
		for (int i = 0; i < count; i++) {
			result += i;
		}
		return result;
	}

}
