package org.ds.chronos.example;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import org.ds.chronos.api.Chronicle;
import org.ds.chronos.api.ChronicleBatch;
import org.ds.chronos.api.ChronologicalRecord;
import org.ds.chronos.api.CassandraChronos;
import org.ds.chronos.api.ChronosException;
import org.ds.chronos.api.PartitionPeriod;
import org.ds.chronos.support.TestData;
import org.ds.chronos.support.TestDecoder;
import org.ds.chronos.support.TestEncoder;
import org.ds.chronos.timeline.Timeline;

@SuppressWarnings("unused")
public class Example {

  public static void main(String[] args) throws ChronosException {
    Calendar calendar = Calendar.getInstance();

    Cluster cluster = HFactory.getOrCreateCluster("chronos", "localhost:9160");
    if (cluster.describeKeyspace("chronos") == null) {
      cluster.addKeyspace(HFactory.createKeyspaceDefinition("chronos"), true);
    }
    Keyspace keyspace = HFactory.createKeyspace("chronos", cluster);

    // Create chronos instance for the column family metrics to use
    // as a factory for accessing Chronicles or Timelines
    CassandraChronos chronos = new CassandraChronos(cluster, keyspace, "metrics");

    // ///////////////////

    Chronicle chronicle = chronos.getChronicle("site-445");

    // ///////////////////

    Chronicle monthly = chronos.getChronicle("site-445", PartitionPeriod.MONTH);

    // ///////////////////

    chronicle.add(new Date(), "log entry");
    chronicle.add(System.currentTimeMillis(), new byte[] { 0x10, 0x50 });

    // ///////////////////

    ChronicleBatch batch = new ChronicleBatch();
    batch.add(System.currentTimeMillis() - 100, "1");
    batch.add(System.currentTimeMillis() - 50, "2");
    batch.add(System.currentTimeMillis() - 20, "1");
    chronicle.add(batch);

    // ///////////////////

    long t1 = System.currentTimeMillis() - 500;
    long t2 = System.currentTimeMillis();

    Iterator<ChronologicalRecord> stream = chronicle.getRange(t1, t2);
    while (stream.hasNext()) {
      ChronologicalRecord column = stream.next();
      Long time = column.getTimestamp();
      String data = new String(column.getData(), Chronicle.CHARSET);
    }

    // ///////////////////

    long count = chronicle.getNumEvents(t1, t2);

    // ///////////////////

    chronicle.deleteRange(t1, t2);

    // ///////////////////

    boolean recorded = chronicle.isEventRecorded(t1);

    // ///////////////////
    // ///////////////////

    Timeline<TestData> timeline = chronos.getTimeline("site-445-data",
        new TestEncoder(), new TestDecoder());

    TestData data = new TestData();
    data.time = new Date().getTime();
    data.type = 0x04;
    data.value = 15d;
    timeline.add(data);

    List<TestData> collection = new ArrayList<TestData>();
    for (int i = 0; i < 10; i++) {
      data = new TestData();
      data.time = new Date().getTime();
      data.type = 0x04;
      data.value = 15d;
      collection.add(data);
    }
    timeline.add(collection);
  }
}
