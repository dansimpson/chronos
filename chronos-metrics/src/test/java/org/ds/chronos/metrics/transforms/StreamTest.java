package org.ds.chronos.metrics.transforms;

import java.util.Calendar;
import java.util.Date;

import org.ds.chronos.api.chronicle.MemoryChronicle;
import org.ds.chronos.metrics.Metric;
import org.ds.chronos.metrics.MetricStore;
import org.ds.chronos.metrics.MetricSummary;
import org.ds.chronos.timeline.stream.partitioned.DurationPredicate;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.ds.chronos.metrics.transforms.MetricFilters.*;

public class StreamTest {

	private static final int months = 3;

	private MemoryChronicle chronicle;
	private MetricStore timeline;

	@Before
	public void createChronicle() {
		chronicle = new MemoryChronicle();
		timeline = new MetricStore(chronicle);

		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.MONTH, -months);

		while (cal.getTimeInMillis() < System.currentTimeMillis()) {
			timeline.add(new Metric(cal.getTimeInMillis(), (float) Math.random() * 200f));
			cal.add(Calendar.MINUTE, 1);
		}
	}

	@Test(timeout = 2000)
	public void testAverage() {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.DATE, -30);
		Date t1 = cal.getTime();
		cal.add(Calendar.DATE, 15);
		Date t2 = cal.getTime();

		Iterable<MetricSummary> stream = timeline.query(t1, t2).partition(new DurationPredicate<Metric>("4h"))
		    .reduceAll(MetricFilters.summarize).streamAs(MetricSummary.class);

		int count = 0;
		for (MetricSummary metric : stream) {
			count++;
			Assert.assertNotNull(metric);
		}

		Assert.assertEquals(6 * 15, count);
	}

	@Test
	public void testGamut() {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.DATE, -30);
		Date begin = cal.getTime();
		cal.add(Calendar.DATE, 15);
		Date end = cal.getTime();

		Iterable<MetricSummary> stream = timeline.query(begin, end)
				.map(sma(30))
				.filter(gte(0f))
		    .partition(new DurationPredicate<Metric>("1h"))
		    .reduceAll(MetricFilters.summarize)
		    .streamAs(MetricSummary.class);
		
		int count = 0;
		for (@SuppressWarnings("unused")
		MetricSummary metric : stream) {
			count++;
		}

		Assert.assertEquals(350, count, 10);
	}
}
