package org.ds.chronos.metrics.archive;

import java.util.Iterator;
import java.util.List;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import org.ds.chronos.api.CassandraChronos;
import org.ds.chronos.api.ChronosException;
import org.ds.chronos.chronicle.PartitionPeriod;
import org.ds.chronos.metrics.Metric;
import org.ds.chronos.timeline.Timeline;
import org.ds.chronos.util.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Bench {

  private static final Logger log = LoggerFactory.getLogger(Bench.class);
  private static CassandraChronos chronos;

  private static void setup(boolean reset) throws ChronosException {
    String keyspaceName = "chronosmetrics";

    Cluster cluster = HFactory.getOrCreateCluster("development",
        "localhost:9160");

    KeyspaceDefinition ksDef = cluster.describeKeyspace(keyspaceName);

    if (ksDef != null && reset) {
      List<ColumnFamilyDefinition> cfDefs = ksDef.getCfDefs();
      for (ColumnFamilyDefinition cfDef : cfDefs) {
        cluster.truncate(keyspaceName, cfDef.getName());
        cluster.dropColumnFamily(keyspaceName, cfDef.getName());
      }
      cluster.dropKeyspace(keyspaceName, true);
      cluster.addKeyspace(HFactory.createKeyspaceDefinition(keyspaceName), true);
    }

    Keyspace keyspace = HFactory.createKeyspace(keyspaceName, cluster);
    chronos = new CassandraChronos(cluster, keyspace, "metricsbench");
  }

  private static Iterator<Metric> generate(final long time, final long count,
      final long period) {

    return new Iterator<Metric>() {

      private int counter = 0;

      @Override
      public boolean hasNext() {
        return counter < count;
      }

      @Override
      public Metric next() {
        return new Metric(time + (period * counter++),
            (float) Math.sin(counter) * 5f);
      }

      @Override
      public void remove() {
      }

    };

  }

  public static void main(String[] args) throws ChronosException {
    setup(false);

    MetricArchive archive = new MetricArchive(chronos.getChronicle("compact",
        PartitionPeriod.MONTH), new Duration("1m"), new Duration("1d"));

    
    log.info("Column count: {}", archive.getNumEvents(0, new Duration("11y").getMillis()));
    log.info("Compacted....");
//    benchWrite(archive, new Duration("1m"), new Duration("10y"), 10);
    
    benchRead(archive, new Duration("30d"), 100);
    benchRead(archive, new Duration("1d"), 1);
    benchRead(archive, new Duration("2d"), 100);
//    benchRead(archive, new Duration("1y"), 20);
//    benchRead(archive, new Duration("10y"), 32);
  }

  public static void benchWrite(Timeline<Metric> store, Duration period,
      Duration length, int batchSize) {
    long time = System.currentTimeMillis();
    log.info("Inserting {} worth of metrics", length.toString());
    store
        .add(
            generate(0, length.getMillis() / period.getMillis(),
                period.getMillis()), batchSize);
    log.info("Completed in {} ms", System.currentTimeMillis() - time);
  }

  public static void benchRead(Timeline<Metric> store, Duration duration,
      int times) {

    log.info("Fetching {} of data", duration.toString());

    for (int i = 0; i < times; i++) {
      Iterator<Metric> stream = store.query(0, duration.getMillis(),
          Metric.class).iterator();

      long time = System.currentTimeMillis();

      int count = 0;
      while (stream.hasNext()) {
        stream.next();
        count++;
      }

      log.info("{} metrics read in {} ms", count, System.currentTimeMillis()
          - time);
    }
  }

}
