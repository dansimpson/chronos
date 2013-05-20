package org.ds.chronos.chronicle;

import java.util.Iterator;

import org.ds.chronos.api.Chronicle;
import org.ds.chronos.api.ChronologicalRecord;

import com.foundationdb.Database;
import com.foundationdb.KeySelector;
import com.foundationdb.KeyValue;
import com.foundationdb.RangeQuery;
import com.foundationdb.ReadTransaction;
import com.foundationdb.StreamingMode;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

public class FdbChronicle extends Chronicle {

	private final Database database;
	private final String key;

	public FdbChronicle(Database database, String key) {
		this.database = database;
		this.key = key;
	}

	@Override
	public void add(final ChronologicalRecord item) {
		Transaction transaction = database.createTransaction();
		transaction.set(getKey(item.getTimestamp()), item.getData());
		transaction.commit().get();
	}

	@Override
	public void add(final Iterator<ChronologicalRecord> items, int pageSize) {
		Transaction transaction = database.createTransaction();
		while (items.hasNext()) {
			ChronologicalRecord record = items.next();
			transaction.set(getKey(record.getTimestamp()), record.getData());
		}
		transaction.commit().get();
	}

	@Override
	public Iterator<ChronologicalRecord> getRange(final long t1, final long t2, int pageSize) {

		ReadTransaction transaction = database.createTransaction();

		RangeQuery query;
		if (t2 < t1) {
			query = transaction.getRange(getKey(t2), getKey(t1)).reverse();
		} else {
			query = transaction.getRange(getKey(t1), getKey(t2));
		}
		query.streamingMode(StreamingMode.SERIAL);

		return Iterators.transform(query.iterator(), new Function<KeyValue, ChronologicalRecord>() {

			@Override
			public ChronologicalRecord apply(KeyValue kv) {
				return new ChronologicalRecord(Tuple.fromBytes(kv.getKey()).getLong(1), kv.getValue());
			}
		});
	}

	@Override
	public long getNumEvents(final long t1, final long t2) {
		return Iterators.size(getRange(t1, t2, 0));
	}

	@Override
	public void delete() {
		Transaction transaction = database.createTransaction();
		transaction.clearRangeStartsWith(Tuple.from(key).pack());
		transaction.commit().get();
	}

	@Override
	public void deleteRange(final long t1, final long t2) {
		Transaction transaction = database.createTransaction();
		transaction.clear(getKey(t1), getKey(t2));
		transaction.clear(getKey(t1));
		transaction.clear(getKey(t2));
		transaction.commit().get();
	}

	@Override
	public boolean isEventRecorded(long time) {
		return database.createTransaction().get(getKey(time)).get() != null;
	}

	private byte[] getKey(long time) {
		return Tuple.from(key, time).pack();
	}
}
