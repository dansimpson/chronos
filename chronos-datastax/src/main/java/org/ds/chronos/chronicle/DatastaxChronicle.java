package org.ds.chronos.chronicle;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.ds.chronos.api.Chronicle;
import org.ds.chronos.api.ChronologicalRecord;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
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
	private static final String CREATE_STATEMENT = "CREATE TABLE %s (name varchar, time bigint, data blob, PRIMARY KEY(name, time)) WITH COMPACT STORAGE";

	protected final String name;
	protected final String table;
	protected final Session session;
	protected final int ttl;

	public DatastaxChronicle(Session session, String table, String name) {
		this(session, table, name, 0);
	}

	public DatastaxChronicle(Session session, String table, String name, int ttl) {
		this.session = session;
		this.table = table;
		this.name = name;
		this.ttl = ttl;

		// Increasing this can decrease throughput
		WRITE_PAGE_SIZE = 64;
	}

	@Override
	public void add(ChronologicalRecord column) {
		Insert query = QueryBuilder.insertInto(table).value("name", name).value("time", column.getTimestamp())
		    .value("data", column.getValueBytes());
		if (ttl > 0) {
			query.using(ttl(ttl));
		}
		session.execute(query);
	}

	@Override
	public void add(Iterator<ChronologicalRecord> items, int pageSize) {

		List<Statement> statements = new ArrayList<Statement>();
		while (items.hasNext()) {
			ChronologicalRecord item = items.next();

			Insert query = QueryBuilder.insertInto(table).value("name", name).value("time", item.getTimestamp())
			    .value("data", item.getValueBytes());
			if (ttl > 0) {
				query.using(ttl(ttl));
			}
			statements.add(new SimpleStatement(query.toString()));

			if (statements.size() >= pageSize) {
				session.execute(QueryBuilder.batch(statements.toArray(new Statement[0])));
				statements.clear();
			}
		}

		if (!statements.isEmpty()) {
			session.execute(QueryBuilder.batch(statements.toArray(new Statement[0])));
		}
	}

	@Override
	public long getNumEvents(long t1, long t2) {
		assert (t1 <= t2);

		Query query = select().countAll().from(table).where(eq("name", name)).and(gte("time", t1)).and(lte("time", t2));
		ResultSet result = session.execute(query);
		if (result.isExhausted()) {
			return 0;
		}
		return result.iterator().next().getLong(0);
	}

	@Override
	public boolean isEventRecorded(long time) {
		Query query = QueryBuilder.select().from(table).where(eq("name", name)).and(eq("time", time));
		return !session.execute(query).isExhausted();
	}

	@Override
	public void delete() {
		Query query = QueryBuilder.delete().from(table).where(eq("name", name));
		session.execute(query);
	}

	@Override
	public Iterator<ChronologicalRecord> getRange(long t1, long t2, int pageSize) {

		long begin = Math.min(t1, t2);
		long end = Math.max(t1, t2);

		Where query = select().column("time").column("data").from(table).where(eq("name", name)).and(gte("time", begin))
		    .and(lte("time", end));

		if (t1 > t2) {
			query.orderBy(QueryBuilder.desc("time"));
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
			session.execute(QueryBuilder.delete().from(table).where(eq("name", name)).and(eq("time", record.getTimestamp())));
		}
	}

	/**
	 * Create the schema for the current session and table name, using compact storage and defaults
	 */
	public void createTable() {
		session.execute(String.format(CREATE_STATEMENT, table));
	}

}
