# Chronos

A small API for storing, traversing, and processing
timeseries data.

#### APIS

* Storage
* Iteration, counting, and deletion for time ranges
* Lazy processing
* Filters, transformations, partitioning, and reducing
* Customized serialization

## General Concept

Creating timelines and adding data:

```java
Timeline<Metric> timeline = new MetricStore(chronicle);
timeline.add(metric);
timeline.add(metrics);
```

Lazily load events and process them through a pipeline
with a simple moving average, threshold filter, and then
reduce the data to into summary for every hour (1h), and finally
iterate over the results.  Example from chronos-metrics:

```java
Iterable<MetricSummary> stream = timeline.query(begin, end)
    .map(sma(30))
    .filter(gte(0f))
    .partition(new DurationPredicate<Metric>("1h"))
    .reduceAll(MetricFilters.summarize)
    .streamAs(MetricSummary.class);

for(MetricSummary metric: stream) {
  System.out.println(metric.getStandardDeviation());
}
```


### Chronicles

A Chronicle is a the base unit of storage for timeseries.  If you
want a higher level API, see the Timeline class.

Chronicle Types:

* MemoryChronicle - Used for tests, etc
* CassandraChronicle - A single row chronicle (see chronos-cassandra)
* PartitionedChronicle - A chronicle partitioned over many chronicles.
  Each event falls on a key based on the event date, for example 
  an event day 2013-01-15 would land on the key: site-445-2013-01.  
  Queries iterate over keys and columns to stream the partitioned time series
  in order (see chronos-cassandra)
* RedisChronicle - Redis backed with Jedis

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

Iterator<ChronologicalRecord> stream = chronicle.getRange(t1, t2);
while(stream.hasNext()) {
  ChronologicalRecord record = stream.next();
  long time = record.getTimestamp();
  byte[] data = record.getData();
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
from ChronologicalRecord objects to a class of your choice and back.  It abstracts away
some of the complexity with pure Chronicles.  A DataStream wraps the FluentIterable class 
from Guava and supports filters, transforms, partitioning, and reducers.  This model supports
lazy loading from the underlying data source.

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

  private Iterator<ChronologicalRecord> input;
  
  @Override
  public void setInputStream(Iterator<ChronologicalRecord> input) {
    this.input = input;
  }

  @Override
  public boolean hasNext() {
    return input.hasNext();
  }

  @Override
  public TestData next() {
    ChronologicalRecord column = input.next();      
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
  public ChronologicalRecord next() {
    TestData data = input.next();
    ByteBuffer buffer = ByteBuffer.allocate(9);
    buffer.put(data.type);
    buffer.putDouble(data.value);
    buffer.rewind();
    return new ChronologicalRecord(data.time, buffer.array());
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
  = new Timeline<TestData>(getChronicle(), new TestEncoder(), new TestDecoder());
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

##### Stream processing

Processing the data stream is possible using 3 functional(ish)
interfaces:

* map(function<I,O> -> O)
* filter(predicate<T> -> boolean)
* partition(predicate<T>) -> PartitionedDataStream<T>
* reduceAll(function<iterator<T>, T>) -> DataStream<T>

Example of a map function which plucks just the tag from the stream
of Observation objects.

```java
new Function<Observation, String>() {
  public String apply(Observation o) {
    return p.getTag();
  }
};
```

Example of a filter, which only emits observations where the tag = "spike"

```java
new Predicate<Observation>() {
  public boolean apply(Observation o) {
    return "spike".equals(p.getTag());
  }
};
```

#### Stiching it together

With the DataStream, it's possible to compose multi stage pipelines
of iterators which apply the map functions and emit a result stream.

```java
Iterable<Double> stream = timeline.query(begin, end)
  .filter(tagFilter)
  .map(multBy2)
  .streamAs(Double.class);

for(Double d: stream) {
  out.write(d);
}
```


Maven
-----

Maven central or other repo is planned.

```xml
<dependency>
  <groupId>org.ds.chronos</groupId>
  <artifactId>chronos-api</artifactId>
  <version>${chronos.version}</version>
</dependency>
```

Modules
-------
* chronos-api - Timeseries processing and storage API
* chronos-jackson - Storing timeseries objects as json
* chronos-metrics - Storing timeseries as numeric values with some transformation utilities
* chronos-cassandra - Scalable timeseries storage and retreival with hector and cassandra
* chronos-redis - Storage with redis
* chronos-aws - Storage with S3/SimpleDB for archiving

Contributing
------------

Just fork it, change it, rebase, and send a pull request.

License
-------
MIT License