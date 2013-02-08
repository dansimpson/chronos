package org.ds.chronos.metrics.archive;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.ds.chronos.api.ChronologicalRecord;
import org.ds.chronos.metrics.Metric;
import org.ds.chronos.util.Duration;
import org.junit.Assert;
import org.junit.Test;


public class CompactMetricCodecTest {

  
  @Test
  public void testDecoder() {
    
    Duration duration = new Duration("1m");
    
    ByteBuffer buffer = ByteBuffer.allocate(4 * Metric.BYTE_SIZE + 16);
    buffer.putLong(0);
    buffer.putLong(duration.getMillis());
    buffer.putFloat(1f);
    buffer.putFloat(2f);
    buffer.putFloat(3f);
    buffer.putFloat(4f);
    
    List<ChronologicalRecord> source = new ArrayList<ChronologicalRecord>();
    source.add(new ChronologicalRecord(0l, buffer.array()));
    
    MetricArchiveDecoder decoder = new MetricArchiveDecoder();
    decoder.setInputStream(source.iterator());

    int itr = 0;
    while(decoder.hasNext()) {
      Metric m = decoder.next();
      Assert.assertEquals(itr + 1, m.getValue(), 0.0f);
      Assert.assertEquals(duration.getMillis() * itr, m.getTime());
      itr++;
    }
  
    Assert.assertEquals(4, itr);
  }
  
  @Test
  public void testEncoder() {
    
    Duration duration = new Duration("1m");
    
    List<Metric> source = new ArrayList<Metric>();
    source.add(new Metric(0l, 1.0f));
    source.add(new Metric(1000l, 2.0f));
    source.add(new Metric(2000l, 3.0f));
    source.add(new Metric(3000l, 4.0f));
    
    MetricArchiveEncoder encoder = new MetricArchiveEncoder(duration, new Duration("2m"));
    encoder.setInputStream(source.iterator());

    int itr = 0;
    while(encoder.hasNext()) {
      ChronologicalRecord data = encoder.next();
      Assert.assertEquals(2 * Metric.BYTE_SIZE + 16, data.getData().length);
      itr++;
    }
  
    Assert.assertEquals(2, itr);
  }
  
}
