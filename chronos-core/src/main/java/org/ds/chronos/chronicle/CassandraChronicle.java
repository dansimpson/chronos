package org.ds.chronos.chronicle;

import java.util.Iterator;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.SliceQuery;

import org.ds.chronos.api.Chronicle;

/**
 * 
 * A chronicle where each event is a column on a single row
 * in cassandra.
 * 
 * @author Dan
 *
 */
public class CassandraChronicle extends Chronicle {

	protected String key;
	protected Keyspace keyspace;
	protected ColumnFamilyTemplate<String, Long> template;

	public CassandraChronicle(Keyspace keyspace,
			ColumnFamilyTemplate<String, Long> template, String key) {
		this.keyspace = keyspace;
		this.template = template;
		this.key = key;
	}

	@Override
	public void add(HColumn<Long, byte[]> column) {
		ColumnFamilyUpdater<String, Long> updater = template.createUpdater(key);
		updater.setColumn(column);
		template.update(updater);
	}

	@Override
	public void add(Iterator<HColumn<Long, byte[]>> items, int pageSize) {
		ColumnFamilyUpdater<String, Long> updater = template.createUpdater(key);
		int count = 0;
		while(items.hasNext()) {
			HColumn<Long, byte[]> column = items.next();
			updater.setColumn(column);
			if (++count % pageSize == 0) {
				template.update(updater);
			}	
		}
		template.update(updater);
	}

	@Override
	public long getNumEvents(long t1, long t2) {
		assert(t1 <= t2);
		return template.countColumns(key, t1, t2, Integer.MAX_VALUE);
	}
	
	@Override
	public boolean isEventRecorded(long time) {
		ColumnQuery<String, Long, byte[]> columnQuery =
			    HFactory.createColumnQuery(keyspace, StringSerializer.get(), LongSerializer.get(), BytesArraySerializer.get());
		columnQuery.setColumnFamily(template.getColumnFamily()).setKey(key).setName(time);
		return columnQuery.execute().get() != null;
	}

	@Override
	public void delete() {
		template.deleteRow(key);
	}
	
	@Override
	public Iterator<HColumn<Long, byte[]>> getRange(long t1, long t2, int pageSize) {
		SliceQuery<String, Long, byte[]> query = HFactory.createSliceQuery(
				keyspace, StringSerializer.get(), LongSerializer.get(), BytesArraySerializer.get());		
		query.setColumnFamily(template.getColumnFamily());
		query.setKey(key);
		query.setRange(t1, t2, t1 > t2, pageSize);
		return new ColumnSliceIterator<String, Long, byte[]>(query, t1, t2, t1 > t2, pageSize);
	}

	@Override
	public void deleteRange(long t1, long t2) {
		assert(t1 <= t2);
		Iterator<HColumn<Long, byte[]>> range = getRange(t1, t2);
		while(range.hasNext()) {
			template.deleteColumn(key, range.next().getName());
		}
	}

}
