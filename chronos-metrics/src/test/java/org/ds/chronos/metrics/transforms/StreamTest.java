package org.ds.chronos.metrics.transforms;

import static org.ds.chronos.metrics.transforms.MetricFilters.gte;
import static org.ds.chronos.metrics.transforms.MetricFilters.sma;
import static org.ds.chronos.metrics.transforms.MetricFilters.summarize;

import java.util.Calendar;
import java.util.Date;

import org.ds.chronos.chronicle.MemoryChronicle;
import org.ds.chronos.metrics.Metric;
import org.ds.chronos.metrics.MetricStore;
import org.ds.chronos.metrics.MetricSummary;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
      timeline.add(new Metric(cal.getTimeInMillis(),
          (float) Math.random() * 200f));
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

    Iterable<MetricSummary> stream = timeline
        .query(t1, t2, MetricSummary.class).aggregate(summarize("4h")).stream();

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

    Iterable<MetricSummary> stream = timeline
        .query(begin, end, MetricSummary.class).transform(sma(30))
        .filter(gte(0f)).aggregate(summarize("1h")).stream();

    int count = 0;
    for (@SuppressWarnings("unused") MetricSummary metric : stream) {
      count++;
    }
    
    Assert.assertEquals(350, count, 10);
  }

}
