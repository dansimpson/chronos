chronos-cassandra
-----------------

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

#### Creating a chronicle

Build a chronicle with data that resides on a single key:

```java
Chronicle chronicle = chronos.getChronicle("site-445");
```

Build a chronicle with data paritioned into months:

```java
Chronicle monthly = chronos.getChronicle("site-445", PartitionPeriod.MONTH);
```