package org.ds.chronos.chronicle;

import org.ds.chronos.api.ChronosException;
import org.junit.BeforeClass;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class TestBase {

	protected static Cluster cluster;
	protected static Session session;

	protected static final String keyspace = "staxtests";
	protected static final String table = "testable";

	@BeforeClass
	public static void setup() throws ChronosException {
		if (cluster == null) {
			cluster = new Cluster.Builder().addContactPoints("127.0.0.1").build();
			session = cluster.connect();

			try {
				session.execute("DROP KEYSPACE " + keyspace);
			} catch (Throwable t) {
			}

			session.execute(String.format(
			    "CREATE KEYSPACE %s WITH replication= {'class':'SimpleStrategy', 'replication_factor':1};", keyspace));
			session.shutdown();
		}

		session = cluster.connect(keyspace);

		getChronicle("test").createTable();
	}

	public static DatastaxChronicle getChronicle(String name) throws ChronosException {
		return new DatastaxChronicle(session, table, name);
	}

}
