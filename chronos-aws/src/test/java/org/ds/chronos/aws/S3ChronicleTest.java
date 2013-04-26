package org.ds.chronos.aws;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.ds.chronos.api.Chronicle;
import org.ds.chronos.api.ChronologicalRecord;
import org.ds.chronos.api.chronicle.MemoryChronicle;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;

public class S3ChronicleTest {

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

	AmazonS3Client client;
	Chronicle chronicle;
	String bucket = "chronos-tests";

	public AmazonS3Client getClient() {
		String ak = System.getenv("AWS_AK");
		String sk = System.getenv("AWS_SK");

		if (ak != null && sk != null) {
			AmazonS3Client client = new AmazonS3Client(new BasicAWSCredentials(ak, sk));
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
		chronicle.delete();
	}

	public S3Chronicle getChronicle() {
		return new S3Chronicle(client, bucket, "test");
	}

	@Test
	public void testAdd() {
		chronicle.add(getTestItem(0));
		Assert.assertEquals(1, chronicle.getNumEvents(0, System.currentTimeMillis()));
		Assert.assertEquals(1, getChronicle().getNumEvents(0, System.currentTimeMillis()));
	}

	@Test
	public void testAddBatch() {
		int items = 1440 * 30;
		int iters = 1;

		chronicle.add(getTestItemList(0, 1000, items));
		Assert.assertEquals(items, chronicle.getNumEvents(0, System.currentTimeMillis()));

		long t1 = System.currentTimeMillis();
		for (int i = 0; i < iters; i++) {
			Assert.assertEquals(items, getChronicle().getNumEvents(0, System.currentTimeMillis()));
		}
		long t2 = System.currentTimeMillis();

		System.out.printf("%d items loaded and decoded %d times in %d ms", items, iters, t2 - t1);
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
