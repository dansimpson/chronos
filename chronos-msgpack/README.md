chronos-msgpack
---------------

### Setup and usage

Create a timeline:

```java
Timeline<MyObject> timeline = new MsgPackTimeline(chronicle, MyObject.class);
```

Add/Fetch/Delete:

```java
timeline.add(new MyObject(...));
timeline.add(myListOfObjects());

Iterable<MyObject> stream 
  = timeline.query(begin, end).streamAs(MyObject.class);

for(MyObject object: stream) {
  //...
}

timeline.deleteRange(begin, end);
timeline.getNumEvents(begin, end);
timeline.isEventRecorded(timestamp);
```
See chronos Timeline for all options and other details.


Maven
-----

```xml
<dependency>
  <groupId>org.ds</groupId>
  <artifactId>chronos-msgpack</artifactId>
  <version>...</version>
</dependency>
```