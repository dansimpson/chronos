package org.ds.chronos.chronicle;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Set;

import org.ds.chronos.api.Chronicle;
import org.ds.chronos.api.ChronologicalRecord;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

public class RedisChronicle extends Chronicle {

	private final JedisPool pool;
	private final byte[] key;

	public RedisChronicle(JedisPool pool, String key) {
		this.pool = pool;
		this.key = key.getBytes();
	}

	private abstract class RedisCommand<T> {

		public T execute() {

			Jedis redis = null;
			try {
				redis = pool.getResource();
				return execute(redis);
			} finally {
				if (redis != null) {
					pool.returnResource(redis);
				}

			}
		}

		protected abstract T execute(Jedis redis);
	}

	private final byte[] encodeRecord(ChronologicalRecord record) {
		ByteBuffer buffer = ByteBuffer.allocate(8 + record.getByteSize());
		buffer.putLong(record.getTimestamp());
		buffer.put(record.getData());
		return buffer.array();
	}

	private final ChronologicalRecord decodeRecord(byte[] raw) {
		ByteBuffer buffer = ByteBuffer.wrap(raw);
		long time = buffer.getLong();
		byte[] data = new byte[buffer.remaining()];
		buffer.get(data);
		return new ChronologicalRecord(time, data);
	}

	@Override
	public void add(final ChronologicalRecord item) {
		new RedisCommand<Void>() {

			@Override
			protected Void execute(Jedis redis) {
				redis.zadd(key, item.getTimestamp(), encodeRecord(item));
				return null;
			}
		}.execute();
	}

	@Override
	public void add(final Iterator<ChronologicalRecord> items, int pageSize) {
		new RedisCommand<Void>() {

			@Override
			protected Void execute(Jedis redis) {
				Pipeline pipeline = redis.pipelined();
				while (items.hasNext()) {
					ChronologicalRecord item = items.next();
					pipeline.zadd(key, item.getTimestamp(), encodeRecord(item));
				}
				pipeline.exec();
				return null;
			}
		}.execute();
	}

	@Override
	public Iterator<ChronologicalRecord> getRange(final long t1, final long t2, int pageSize) {

		Set<byte[]> raw = new RedisCommand<Set<byte[]>>() {

			@Override
			protected Set<byte[]> execute(Jedis redis) {
				if (t1 <= t2) {
					return redis.zrangeByScore(key, t1, t2);
				}
				return redis.zrevrangeByScore(key, t1, t2);
			}
		}.execute();

		return Iterators.transform(raw.iterator(), new Function<byte[], ChronologicalRecord>() {

			@Override
			public ChronologicalRecord apply(byte[] raw) {
				return decodeRecord(raw);
			}
		});
	}

	@Override
	public long getNumEvents(final long t1, final long t2) {
		return new RedisCommand<Long>() {

			@Override
			protected Long execute(Jedis redis) {
				return redis.zcount(key, t1, t2);
			}
		}.execute();
	}

	@Override
	public void delete() {
		new RedisCommand<Void>() {

			@Override
			protected Void execute(Jedis redis) {
				redis.del(key);
				return null;
			}
		}.execute();
	}

	@Override
	public void deleteRange(final long t1, final long t2) {
		new RedisCommand<Void>() {

			@Override
			protected Void execute(Jedis redis) {
				redis.zremrangeByScore(key, t1, t2);
				return null;
			}
		}.execute();
	}

}
