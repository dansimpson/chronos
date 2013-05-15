# chronos-redis

Timeseries storage using redis sorted sets.

### Setup and usage

Create a Chronicle:

```java
Chronicle chronicle = new RedisChronicle(JedisPool pool, String key);
```

See chronos-api for abstractions.

Maven
-----

```xml
<dependency>
  <groupId>org.ds.chronos</groupId>
  <artifactId>chronos-redis</artifactId>
  <version>version</version>
</dependency>
```