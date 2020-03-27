package org.ds.chronos.chronicle;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.util.Iterator;

import org.ds.chronos.api.Chronicle;
import org.ds.chronos.api.ChronologicalRecord;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

/**
 * 
 * A chronicle backed by cassandra and assisted by the Datastax java client.
 * 
 * @author Dan Simpson
 * 
 */
public class DatastaxChronicle extends Chronicle {

	/**
	 * Compact storage tends to be faster for this type of data
	 */
	private static final String CREATE_STATEMENT = "CREATE TABLE %s (%s varchar, %s bigint, %s blob, PRIMARY KEY(%2$s, %3$s)) WITH COMPACT STORAGE";

	/**
	 * Structure to define Table, Key, Clustering Column, Data names
	 * 
	 * @author Dan Simpson
	 *
	 */
	public static final class Settings {

		protected final String table;
		protected final String key;
		protected final String cluster;
		protected final String data;

		private PreparedStatement insert;

		public Settings(String table, String key, String cluster, String data) {
			super();
			this.table = table;
			this.key = key; // series
			this.cluster = cluster; // timestamp
			this.data = data; // value

		}

		protected PreparedStatement insert(Session session) {
			if (insert == null) {
				synchronized (this) {
					insert = session.prepare(
					    String.format("INSERT INTO %s (%s, %s, %s) VALUES (?, ?, ?) USING TTL ?", table, key, cluster, data));
				}
			}
			return insert;
		}

		public static Settings legacy(String table) {
			return new Settings(table, "key", "column1", "value");
		}

		public static Settings modern(String table) {
			return new Settings(table, "name", "time", "data");
		}

	}

	protected final String name;
	protected final Settings table;
	protected final Session session;
	protected final int ttl;

	public DatastaxChronicle(Session session, Settings table, String name) {
		this(session, table, name, 0);
	}

	public DatastaxChronicle(Session session, Settings table, String name, int ttl) {
		this.session = session;
		this.table = table;
		this.name = name;
		this.ttl = ttl;

		// Increasing this can decrease throughput
		WRITE_PAGE_SIZE = 2048;
	}

	@Override
	public void add(ChronologicalRecord column) {
		session.execute(table.insert(session).bind(name, column.getTimestamp(), column.getValueBytes(), ttl));
	}

	@Override
	public void add(Iterator<ChronologicalRecord> items, int pageSize) {
		PreparedStatement insert = table.insert(session);

		BatchStatement statement = new BatchStatement();
		while (items.hasNext()) {
			ChronologicalRecord item = items.next();
			statement.add(insert.bind(name, item.getTimestamp(), item.getValueBytes(), ttl));
			if (statement.size() >= pageSize) {
				session.execute(statement);
				statement.clear();
			}
		}

		if (statement.size() > 0) {
			session.execute(statement);
		}
	}

	@Override
	public long getNumEvents(long t1, long t2) {
		assert (t1 <= t2);

		Where query = select().countAll().from(table.table).where(eq(table.key, name)).and(gte(table.cluster, t1))
		    .and(lte(table.cluster, t2));
		ResultSet result = session.execute(query);
		if (result.isExhausted()) {
			return 0;
		}
		return result.iterator().next().getLong(0);
	}

	@Override
	public boolean isEventRecorded(long time) {
		Where query = QueryBuilder.select().from(table.table).where(eq(table.key, name)).and(eq(table.cluster, time));
		return !session.execute(query).isExhausted();
	}

	@Override
	public void delete() {
		Delete.Where query = QueryBuilder.delete().from(table.table).where(eq(table.key, name));
		session.execute(query);
	}

	@Override
	public Iterator<ChronologicalRecord> getRange(long t1, long t2, int pageSize) {

		long begin = Math.min(t1, t2);
		long end = Math.max(t1, t2);

		Where query = select().column(table.cluster).column(table.data).from(table.table).where(eq(table.key, name))
		    .and(gte(table.cluster, begin)).and(lte(table.cluster, end));

		if (t1 > t2) {
			query.orderBy(QueryBuilder.desc(table.cluster));
		}

		ResultSet set = session.execute(query);
		return Iterators.transform(set.iterator(), new Function<Row, ChronologicalRecord>() {

			public ChronologicalRecord apply(Row row) {
				return new ChronologicalRecord(row.getLong(0), row.getBytes(1));
			}
		});
	}

	@Override
	public void deleteRange(long t1, long t2) {
		assert (t1 <= t2);
		Iterator<ChronologicalRecord> range = getRange(t1, t2);
		while (range.hasNext()) {
			ChronologicalRecord record = range.next();
			session.execute(QueryBuilder.delete().from(table.table).where(eq(table.key, name))
			    .and(eq(table.cluster, record.getTimestamp())));
		}
	}

	/**
	 * Create the schema for the current session and table name, using compact storage and defaults
	 */
	public static void createTable(Session session, Settings settings) {
		session.execute(String.format(CREATE_STATEMENT, settings.table, settings.key, settings.cluster, settings.data));
	}

}
