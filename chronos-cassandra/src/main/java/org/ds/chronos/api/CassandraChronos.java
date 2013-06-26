package org.ds.chronos.api;

import java.util.List;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import org.ds.chronos.chronicle.CassandraChronicle;
import org.ds.chronos.chronicle.CassandraPartitionedChronicle;
import org.ds.chronos.timeline.SimpleTimeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating Chronicles and Timelines.
 * 
 * @author Dan Simpson
 * 
 */
public class CassandraChronos {

	private static final Logger log = LoggerFactory.getLogger(CassandraChronos.class);

	private Cluster cluster;
	private Keyspace keyspace;
	private ColumnFamilyTemplate<String, Long> template;

	public CassandraChronos(Cluster cluster, Keyspace keyspace, String cfName) throws ChronosException {
		this.cluster = cluster;
		this.keyspace = keyspace;
		this.template = createTemplate(cfName);
	}

	public CassandraChronos(Cluster cluster, Keyspace keyspace, ColumnFamilyTemplate<String, Long> template) {
		this.cluster = cluster;
		this.keyspace = keyspace;
		this.template = template;
	}

	public String getKeyspaceName() {
		return keyspace.getKeyspaceName();
	}

	public CassandraChronicle getChronicle(String key) throws ChronosException {
		return new CassandraChronicle(keyspace, template, key);
	}

	public CassandraPartitionedChronicle getChronicle(String key, PartitionPeriod period) throws ChronosException {
		return new CassandraPartitionedChronicle(keyspace, template, key, period);
	}

	public <T extends Temporal> SimpleTimeline<T> getTimeline(String key, TimelineEncoder<T> encoder,
	    TimelineDecoder<T> decoder) throws ChronosException {
		return new SimpleTimeline<T>(getChronicle(key), decoder, encoder);
	}

	public <T extends Temporal> SimpleTimeline<T> getTimeline(String key, TimelineEncoder<T> encoder,
	    TimelineDecoder<T> decoder, PartitionPeriod period) throws ChronosException {
		return new SimpleTimeline<T>(getChronicle(key, period), decoder, encoder);
	}

	private ColumnFamilyTemplate<String, Long> createTemplate(String cfName) throws ChronosException {

		KeyspaceDefinition ksDef = cluster.describeKeyspace(getKeyspaceName());

		if (ksDef == null) {
			throw new ChronosException("Keyspace not defined!");
		}

		ColumnFamilyDefinition cfDef = null;

		List<ColumnFamilyDefinition> cfDefs = ksDef.getCfDefs();
		for (ColumnFamilyDefinition def : cfDefs) {
			if (cfName.equals(def.getName())) {
				cfDef = def;
			}
		}

		if (cfDef == null) {

			log.info("Creating column family {}", cfName);

			cfDef = HFactory.createColumnFamilyDefinition(getKeyspaceName(), cfName, ComparatorType.LONGTYPE);

			cfDef.setKeyValidationClass(ComparatorType.ASCIITYPE.getClassName());

			cluster.addColumnFamily(cfDef, true);
		}

		if (cfDef.getComparatorType() != ComparatorType.LONGTYPE) {
			throw new ChronosException("Column family exists, but does not use Long comparator!");
		}

		if (!cfDef.getKeyValidationClass().equals(ComparatorType.ASCIITYPE.getClassName())) {
			throw new ChronosException("Column family exists, but key validation type is not Ascii!");
		}

		return new ThriftColumnFamilyTemplate<String, Long>(keyspace, cfName, StringSerializer.get(), LongSerializer.get());
	}

}
