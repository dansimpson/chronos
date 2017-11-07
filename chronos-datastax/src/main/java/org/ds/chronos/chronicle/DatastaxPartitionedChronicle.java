package org.ds.chronos.chronicle;

import org.ds.chronos.api.Chronicle;
import org.ds.chronos.api.PartitionPeriod;
import org.ds.chronos.api.PartitionedChronicle;
import org.ds.chronos.chronicle.DatastaxChronicle.Settings;

import com.datastax.driver.core.Session;

public class DatastaxPartitionedChronicle extends PartitionedChronicle {

	protected Session session;
	protected Settings settings;

	public DatastaxPartitionedChronicle(Session session, Settings settings, String keyPrefix, PartitionPeriod period) {
		super(keyPrefix, period);
		this.session = session;
		this.settings = settings;
	}

	@Override
	protected Chronicle getPartition(String key) {
		return new DatastaxChronicle(session, settings, key);
	}

}