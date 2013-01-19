package org.ds.chronos.metrics;

import org.ds.chronos.metrics.MetricSummary;
import org.ds.chronos.metrics.Metric;
import org.junit.Assert;
import org.junit.Test;

public class MetricTest {
	
	@Test
	public void testAggregate() {
		MetricSummary metric = new MetricSummary();
		metric.add(new Metric(1, 10f));
		Assert.assertEquals(0, metric.getDuration());
		Assert.assertEquals(1, metric.getTime());
		Assert.assertEquals(1, metric.getCount());
		Assert.assertEquals(10f, metric.getSum(), 0.0);
		Assert.assertEquals(100f, metric.getSumSquared(), 0.0);
		Assert.assertEquals(0f, metric.getStandardDeviation(), 0.0);
		Assert.assertEquals(10f, metric.getMean(), 0.0);
		Assert.assertEquals(10f, metric.getMin(), 0.0);
		Assert.assertEquals(10f, metric.getMax(), 0.0);
	}
	
	@Test
	public void testAggregateAdd() {
		MetricSummary metric = new MetricSummary();
		metric.add(new Metric(1, 10f));
		metric.add(new Metric(2, 20f));
		Assert.assertEquals(1, metric.getDuration());
		Assert.assertEquals(1, metric.getTime());
		Assert.assertEquals(2, metric.getCount());
		Assert.assertEquals(30f, metric.getSum(), 0.0);
		Assert.assertEquals(100f + 400f, metric.getSumSquared(), 0.0);
		Assert.assertEquals(5f, metric.getStandardDeviation(), 0.0);
		Assert.assertEquals(15f, metric.getMean(), 0.0);
		Assert.assertEquals(10f, metric.getMin(), 0.0);
		Assert.assertEquals(20f, metric.getMax(), 0.0);
	}
}
