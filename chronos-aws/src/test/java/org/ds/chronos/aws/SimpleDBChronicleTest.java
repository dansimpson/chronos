package org.ds.chronos.aws;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.ds.chronos.api.ChronologicalRecord;
import org.ds.chronos.api.chronicle.MemoryChronicle;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;

public class SimpleDBChronicleTest {

	protected ChronologicalRecord getTestItem(long time) {
		return new ChronologicalRecord(time, "Hello".getBytes());
	}

	protected List<ChronologicalRecord> getTestItemList(long startTime, long periodInMillis, int count) {
		List<ChronologicalRecord> result = new ArrayList<ChronologicalRecord>();
		for (int i = 0; i < count; i++) {
			result.add(getTestItem(startTime + (periodInMillis * i)));
		}
		return result;
	}

	AmazonSimpleDBClient client;
	SimpleDBChronicle chronicle;
	String bucket = "chronos-tests";

	public AmazonSimpleDBClient getClient() {
		String ak = System.getenv("AWS_AK");
		String sk = System.getenv("AWS_SK");

		if (ak != null && sk != null) {
			AmazonSimpleDBClient client = new AmazonSimpleDBClient(new BasicAWSCredentials(ak, sk));
			client.setRegion(Region.getRegion(Regions.US_WEST_1));
			return client;
		} else {
			System.err.println("AWS Credentials not set in env");
		}

		return null;
	}

	@Before
	public void setup() {
		client = getClient();

		Assume.assumeTrue(client != null);

		chronicle = getChronicle();

		try {
			chronicle.delete();
			chronicle.createDomain();
		} catch (Throwable t) {
			System.err.println("Can't manipulate domain");
		}

	}

	@After
	public void cleanup() {
		try {
			chronicle.delete();
		} catch (Throwable t) {
			System.err.println("Can't delete domain");
		}
	}

	public SimpleDBChronicle getChronicle() {
		return new SimpleDBChronicle(client, "chronos-test");
	}

	@Test
	public void testAdd() {
		chronicle.add(getTestItem(0));
		Assert.assertEquals(1, chronicle.getNumEvents(0, System.currentTimeMillis()));
		Assert.assertEquals(1, getChronicle().getNumEvents(0, System.currentTimeMillis()));
	}

	@Test
	public void testRange() {
		chronicle.add(getTestItemList(0, 1000, 100));

		List<ChronologicalRecord> items = MemoryChronicle.toList(getChronicle().getRange(1000, 5000));

		Assert.assertEquals(5, items.size());
		Assert.assertEquals(1000, items.get(0).getTimestamp());
		Assert.assertEquals(5000, items.get(4).getTimestamp());
	}

	@Test
	public void testReverseRange() {
		chronicle.add(getTestItemList(0, 1000, 100));

		List<ChronologicalRecord> items = MemoryChronicle.toList(getChronicle().getRange(5000, 1000));

		Assert.assertEquals(5, items.size());
		Assert.assertEquals(5000, items.get(0).getTimestamp());
		Assert.assertEquals(1000, items.get(4).getTimestamp());
	}

	@Test
	public void testCount() {
		chronicle.add(getTestItemList(0, 1000, 100));
		Assert.assertEquals(50, getChronicle().getNumEvents(1, 50000));
	}

}
