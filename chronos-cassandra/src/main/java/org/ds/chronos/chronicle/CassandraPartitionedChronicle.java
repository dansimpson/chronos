package org.ds.chronos.chronicle;

import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.hector.api.Keyspace;

import org.ds.chronos.api.CassandraChronos;
import org.ds.chronos.api.Chronicle;
import org.ds.chronos.api.PartitionPeriod;
import org.ds.chronos.api.PartitionedChronicle;
import org.ds.chronos.timeline.SimpleTimeline;

/**
 * 
 * CassandraPartitionedChronicle
 * <p>
 * A cassandra chronicle which partitions data onto many keys.
 * <p>
 * 
 * @see SimpleTimeline
 * @see PartitionPeriod
 * @see PartitionedChronicle
 * @see CassandraChronos#getChronicle(String, PartitionPeriod)
 * 
 * @author Dan Simpson
 * 
 */
public class CassandraPartitionedChronicle extends PartitionedChronicle {

	protected Keyspace keyspace;
	protected ColumnFamilyTemplate<String, Long> template;

	public CassandraPartitionedChronicle(Keyspace keyspace, ColumnFamilyTemplate<String, Long> template,
	    String keyPrefix, PartitionPeriod period) {
		super(keyPrefix, period);
		this.keyspace = keyspace;
		this.template = template;
	}

	@Override
	protected Chronicle getPartition(String key) {
		return new CassandraChronicle(keyspace, template, key);
	}

}
