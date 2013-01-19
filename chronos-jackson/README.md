chronos-jackson
---------------

chronos + jackson = storage timeseries objects that
can map to json and back

### Setup and usage

Create factory:

```java
JsonTimelineFactory factory = new JsonTimelineFactory(chronos);
```

Create a timeline:

```java
TypeReference<MyObject> typeRef = new TypeReference<MyObject>() {};
Timeline<MyObject> timeline = factory.createTimeline("events-xyz", typeRef);
```
A type ref must be passed in to avoid erasure :(

Add/Fetch/Delete:

```java
timeline.add(new MyObject(...));
timeline.add(myListOfObjects());

Iterable<MyObject> stream 
  = timeline.query(begin, end, MyObject.class)
            .filter(tagfilter("good", "ok")) // custom FilterFn<MyObject>
            .stream();

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
  <artifactId>chronos-jackson</artifactId>
  <version>1.0.0</version>
</dependency>
```