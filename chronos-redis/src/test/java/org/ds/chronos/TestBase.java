package org.ds.chronos;

import java.util.TimeZone;

import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.junit.BeforeClass;

import redis.clients.jedis.JedisPool;

public class TestBase {

	private static JedisPool pool;

	@BeforeClass
	public static void setupBasic() {
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
	}

	public static JedisPool getPool() {
		if (pool == null) {
			Config config = new Config();
			config.testOnBorrow = true;
			config.maxActive = 4;
			config.maxWait = 2000;
			pool = new JedisPool(config, "localhost");
		}
		return pool;
	}

}
