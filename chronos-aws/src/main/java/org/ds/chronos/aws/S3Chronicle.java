package org.ds.chronos.aws;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.ds.chronos.api.Chronicle;
import org.ds.chronos.api.ChronologicalRecord;
import org.ds.chronos.api.chronicle.MemoryChronicle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

/**
 * 
 * A chronicle backed by an S3 Object.
 * <p>
 * The motivation for this implementation is archive related. S3Chronicle should not be used for write heavy workloads, as the entire object
 * needs to be fetched in order to write a single value. See {@link S3PartitionedChronicle} for a more robust implementation, which can
 * partition data by date, into multiple keys.
 * <p>
 * 
 * TODO: Optimize getRange with a streaming decoder.
 * 
 * @author Dan Simpson
 * 
 */
public class S3Chronicle extends Chronicle {

	private static final int BUFSIZE = 1024;
	private static final Logger log = LoggerFactory.getLogger(S3Chronicle.class);

	final AmazonS3Client client;
	final String bucket;
	final String key;

	final MemoryChronicle records;

	public S3Chronicle(AmazonS3Client client, String bucket, String key) {
		this.client = client;
		this.bucket = bucket;
		this.key = key;
		this.records = new MemoryChronicle();
	}

	@Override
	public void add(ChronologicalRecord item) {
		load();
		records.add(item);
		save();
	}

	@Override
	public void add(Iterator<ChronologicalRecord> items, int pageSize) {
		load();
		records.add(items, pageSize);
		save();
	}

	@Override
	public Iterator<ChronologicalRecord> getRange(long t1, long t2, int pageSize) {
		load();
		return records.getRange(t1, t2, pageSize);
	}

	@Override
	public long getNumEvents(long t1, long t2) {
		load();
		return records.getNumEvents(t1, t2);
	}

	@Override
	public void delete() {
		client.deleteObject(bucket, key);
	}

	@Override
	public void deleteRange(long t1, long t2) {
		load();
		records.deleteRange(t1, t2);
		save();
	}

	/**
	 * Load the data from S3, decode it, and store it in the memory chronicle
	 */
	private synchronized void load() {
		if (!records.isEmpty()) {
			return;
		}

		ByteBuffer buffer = getBytes();

		if (buffer.remaining() < 4) {
			return;
		}

		long count = buffer.getInt();
		for (int i = 0; i < count; i++) {
			long time = buffer.getLong();
			byte[] data = new byte[buffer.getInt()];
			buffer.get(data);
			records.add(new ChronologicalRecord(time, data));
		}
	}

	private synchronized void save() {
		ByteArrayDataOutput out = ByteStreams.newDataOutput();
		out.writeInt(records.size());

		for (ChronologicalRecord record : records.all()) {
			out.writeLong(record.getTimestamp());
			out.writeInt(record.getByteSize());
			out.write(record.getData());
		}

		byte[] result = out.toByteArray();

		ObjectMetadata meta = new ObjectMetadata();
		meta.setContentLength(result.length);
		meta.setContentType("application/octet-stream");

		client.putObject(bucket, key, new ByteArrayInputStream(result), meta);
	}

	/**
	 * Convert an S3 object to a ByteBuffer
	 * 
	 * @return
	 */
	private ByteBuffer getBytes() {
		S3Object object;
		try {
			object = client.getObject(bucket, key);
		} catch (Throwable t) {
			return ByteBuffer.allocate(0);
		}

		S3ObjectInputStream stream = object.getObjectContent();

		ByteArrayOutputStream buffer = new ByteArrayOutputStream();

		try {
			int size;
			byte[] tmp = new byte[BUFSIZE];
			while ((size = stream.read(tmp)) != -1) {
				buffer.write(tmp, 0, size);
			}
			stream.close();
		} catch (IOException e) {
			log.warn("Error closing S3 stream");
			log.debug("IOException on S3 close", e);
		}

		return ByteBuffer.wrap(buffer.toByteArray());
	}

}
