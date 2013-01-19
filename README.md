Chronos
=======

A small collection of abstractions for storing, traversing, and processing
timeseries data in cassandra with hector.

Features:
* Events stored as a LongType,ByteArray column
* Traverse, count, and delete items for a given time frame
* Bulk inserts/updates
* Date based paritioning (multiple rows for single data set)
* Lazy streaming via iterators and paging
* API for custom streaming serialization/deserialization
* API for filtering, transforming, and aggregating data lazily

General Concept
---------------

Creating timelines and adding data:

```java
Timeline<Metric> timeline 
  = chronos.getTimeline("metrics-c12345", new MetricEncoder(), new MetricEncoder());
timeline.add(getNewMetrics());
```

Lazily load events and process them through a pipeline
with a simple moving average, threshold filter, and then
reduce the data to into summary for every hour (1h), and finally
iterate over the results.  Example from chronos-metrics:

```java
Iterable<MetricSummary> stream = timeline.query(begin, end, MetricSummary.class)
  .transform(sma(30))
  .filter(gte(0f))
  .aggregate(summarize("1h"))
  .stream();

for(MetricSummary metric: stream) {
  System.out.println(metric.getStandardDeviation());
}
```

Getting Started
---------------

### Configuration

Configuration is done via hector's configuration facilities.  Once setup, you
can create a Chronos factory object, which is used to build Chronicles and Timelines:

```java
Cluster cluster = HFactory.getOrCreateCluster(clusterName, "localhost:9160");
if (cluster.describeKeyspace("chronos") == null) {
  cluster.addKeyspace(HFactory.createKeyspaceDefinition("chronos"), true);
}
Keyspace keyspace = HFactory.createKeyspace("chronos", cluster);

// Chronicle and Timeline factory
Chronos chronos = new Chronos(cluster, keyspace, "metrics");
```

### Chronicles

A Chronicle is a the base unit of storage for timeseries.  If you
want a higher level API, see the Timeline section.

Chronicle Types:

* CassandraChronicle - A single row chronicle
* PartitionedChronicle - A chronicle partitioned over many rows.
  Each event falls on a key based on the event date, for example 
  an event day 2013-01-15 would land on the key: site-445-2013-01.  
  Queries iterate over keys and columns to stream the partitioned time series
  in order
* MemoryChronicle - Used for tests, etc

##### Creating a chronicle

Build a chronicle with data that resides on a single key:

```java
Chronicle chronicle = chronos.getChronicle("site-445");
```

Build a chronicle with data paritioned into months:

```java
Chronicle monthly = chronos.getChronicle("site-445", PartitionPeriod.MONTH);
```

##### Adding data

```java
chronicle.add(new Date(), "log entry");
chronicle.add(System.currentTimeMillis(), new byte[] { 0x10, 0x50 });
```

Add in bulk:

```java
ChronicleBatch batch = new ChronicleBatch();
batch.add(System.currentTimeMillis() - 100, "1");
batch.add(System.currentTimeMillis() - 50, "2");
batch.add(System.currentTimeMillis() - 20, "1");    
chronicle.add(batch);
```

##### Streaming data

```java
long t1 = System.currentTimeMillis() - 500;
long t2 = System.currentTimeMillis();

Iterator<HColumn<Long, byte[]>> stream = chronicle.getRange(t1, t2);
while(stream.hasNext()) {
  HColumn<Long, byte[]> column = stream.next();
  Long time = column.getName();
  String data = new String(column.getValue(), Chronicle.CHARSET);
}
```

##### Counting

```java
long count = chronicle.getNumEvents(t1, t2);
```

##### Deleting

```java
chronicle.deleteRange(t1, t2);
```


##### Record check

```java
boolean recorded = chronicle.isEventRecorded(t1);
```

### Timelines and DataStreams

A Timeline wraps a Chronicle and provides streaming encoding/decoding
from hector columns to class of your choice and back.  It abstracts away
some of the complexity with pure Chronicles.  A DataStream chained iterator
which supports filters, transforms, maps, and reducers.  The data is lazy loaded
from cassandra in blocks and as it propogates down the chain, is mutated.

##### Create an encoder/decoder

Example class:

```java
public class TestData {
  public long time;
  public byte type;
  public double value;
}
```

Create a decoder which streams columns from a chronicle
and outputs TestData objects:

```java
public class TestDecoder implements TimelineDecoder<TestData> {

  private Iterator<HColumn<Long, byte[]>> input;
  
  @Override
  public void setInputStream(Iterator<HColumn<Long, byte[]>> input) {
    this.input = input;
  }

  @Override
  public boolean hasNext() {
    return input.hasNext();
  }

  @Override
  public TestData next() {
    HColumn<Long, byte[]> column = input.next();      
    ByteBuffer buffer = column.getValueBytes();
    
    TestData data = new TestData();
    data.time = column.getName();
    data.type = buffer.get();
    data.value = buffer.getDouble();
    return data;
  }

  @Override
  public void remove() {
  }

}
```

Create an encoder which encodes TestData objects as columns:

```java
public class TestEncoder implements TimelineEncoder<TestData> {

  private Iterator<TestData> input;
  
  @Override
  public boolean hasNext() {
    return input.hasNext();
  }

  @Override
  public HColumn<Long, byte[]> next() {
    TestData data = input.next();
    ByteBuffer buffer = ByteBuffer.allocate(9);
    buffer.put(data.type);
    buffer.putDouble(data.value);
    buffer.rewind();
    return HFactory.createColumn(data.time, buffer.array());
  }

  @Override
  public void remove() {
  }

  @Override
  public void setInputStream(Iterator<TestData> input) {
    this.input = input;
  }
}
```

##### Create a Timeline

```java
Timeline<TestData> timeline
  = chronos.getTimeline("site-445-data", new TestEncoder(), new TestDecoder());
```

##### Add objects

Add an object:

```java
timeline.add(buildTestData());
```

Bulk add objects:

```java
timeline.add(buildTestDataCollection());
```

Alternate bulk add with specified batch size:

```java
timeline.add(buildTestDataCollection().iterator(), 5);
```
The above shows the use of iterators, which we can take advantage
of using timelines as an input for transfer.

##### Streaming and processing

```java
```

Maven
-----

```xml
<dependency>
  <groupId>org.ds</groupId>
  <artifactId>chronos</artifactId>
  <version>1.0.0</version>
</dependency>
```

Modules
-------
* chronos-core - Scalable timeseries storage and retreival with hector
* chronos-jackson - Storing timeseries objects as json
* chronos-metrics - Storing timeseries as numeric values with some transformation utilities

Contributing
------------

Just fork it, change it, rebase, and send a pull request.

License
-------
MIT License