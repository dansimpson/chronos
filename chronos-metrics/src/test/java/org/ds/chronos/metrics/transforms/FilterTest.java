package org.ds.chronos.metrics.transforms;

import static org.ds.chronos.metrics.transforms.MetricFilters.buffer;
import static org.ds.chronos.metrics.transforms.MetricFilters.gte;
import static org.ds.chronos.metrics.transforms.MetricFilters.lte;
import static org.ds.chronos.metrics.transforms.MetricFilters.multiply;
import static org.ds.chronos.metrics.transforms.MetricFilters.range;
import static org.ds.chronos.metrics.transforms.MetricFilters.sma;

import java.util.LinkedList;

import org.ds.chronos.metrics.Metric;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Function;

public class FilterTest {

	@Test
	public void testGte() {
		Metric metric = new Metric(0l, 1.0f);
		Assert.assertTrue(gte(0.0f).apply(metric));
		Assert.assertFalse(gte(7.0f).apply(metric));
	}

	@Test
	public void testLte() {
		Metric metric = new Metric(0l, 1.0f);
		Assert.assertFalse(lte(0.0f).apply(metric));
		Assert.assertTrue(lte(7.0f).apply(metric));
	}

	@Test
	public void testRange() {
		Metric metric = new Metric(0l, 1.0f);
		Assert.assertTrue(range(0.0f, 5.0f).apply(metric));
		Assert.assertFalse(range(2.0f, 5.0f).apply(metric));
	}

	@Test
	public void testMultiply() {
		Metric metric = new Metric(0l, 3.0f);
		Assert.assertEquals(15.0f, multiply(5.0f).apply(metric).getValue(), 0.0f);
	}

	@Test
	public void testSma() {
		Function<Metric, Metric> sma = sma(2);
		Assert.assertEquals(3.0f, sma.apply(new Metric(0l, 3.0f)).getValue(), 0.0f);
		Assert.assertEquals(4.0f, sma.apply(new Metric(0l, 5.0f)).getValue(), 0.0f);
		Assert.assertEquals(6.0f, sma.apply(new Metric(0l, 7.0f)).getValue(), 0.0f);
		Assert.assertEquals(7.0f, sma.apply(new Metric(0l, 7.0f)).getValue(), 0.0f);
		Assert.assertEquals(10.0f, sma.apply(new Metric(0l, 13.0f)).getValue(), 0.0f);
	}

	@Test
	public void testBuffer() {
		Metric metric = new Metric(0l, 3.0f);
		Function<Metric, LinkedList<Metric>> buf = buffer(2);

		buf.apply(metric);
		buf.apply(metric);
		buf.apply(metric);

		for (int i = 0; i < 20; i++) {
			Assert.assertEquals(2, buf.apply(metric).size());
		}
	}

}
