package org.ds.chronos.metrics;

import java.util.ArrayList;
import java.util.List;

import org.ds.chronos.api.chronicle.MemoryChronicle;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MetricStoreTest {

  private MemoryChronicle chronicle;

  @Before
  public void createChronicle() {
    chronicle = new MemoryChronicle();
  }

  @Test
  public void testAdd() {
    MetricStore store = new MetricStore(chronicle);
    store.add(new Metric(0, 10f));
    Assert.assertEquals(1,
        chronicle.getNumEvents(0, System.currentTimeMillis()));
  }

  @Test
  public void testAddBlock() {
    MetricStore store = new MetricStore(chronicle);
    List<Metric> metrics = new ArrayList<Metric>();
    for (int i = 0; i < 128; i++) {
      metrics.add(new Metric(i * 1000, 10f));
    }
    store.add(metrics);
    Assert.assertEquals(128,
        chronicle.getNumEvents(0, System.currentTimeMillis()));
  }

  @Test
  public void testFetch() {
    MetricStore store = new MetricStore(chronicle);
    List<Metric> metrics = new ArrayList<Metric>();
    for (int i = 0; i < 128; i++) {
      metrics.add(new Metric(i * 1000, 10f));
    }
    store.add(metrics);

    List<Metric> set = store.query(0, 128000).toList();
    Assert.assertEquals(128, set.size());
  }

  @Test
  public void testFetchWithBoundaryConditions() {
    MetricStore store = new MetricStore(chronicle);
    List<Metric> metrics = new ArrayList<Metric>();
    for (int i = 0; i < 128; i++) {
      metrics.add(new Metric(i * 1000, 10f));
    }
    store.add(metrics);

    List<Metric> set = store.query(64000, 128000).toList();
    Assert.assertEquals(64, set.size());
  }

}
