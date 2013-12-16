package org.ds.chronos.aws;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.ds.chronos.api.Chronicle;
import org.ds.chronos.api.ChronologicalRecord;
import org.ds.chronos.api.chronicle.MemoryChronicle;

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

	final AmazonS3Client client;
	final String bucket;
	final String key;

	final MemoryChronicle records;
	final boolean gzip;

	public S3Chronicle(AmazonS3Client client, String bucket, String key) {
		this(client, bucket, key, false);
	}

	public S3Chronicle(AmazonS3Client client, String bucket, String key, boolean gzip) {
		this.client = client;
		this.bucket = bucket;
		this.key = key;
		this.gzip = gzip;
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
		byte[] result = encode();

		ObjectMetadata meta = new ObjectMetadata();
		meta.setContentLength(result.length);
		meta.setContentType("application/octet-stream");

		client.putObject(bucket, key, new ByteArrayInputStream(result), meta);
	}

	protected byte[] encode() {
		ByteArrayDataOutput out = ByteStreams.newDataOutput();
		out.writeInt(records.size());

		for (ChronologicalRecord record : records.all()) {
			out.writeLong(record.getTimestamp());
			out.writeInt(record.getByteSize());
			out.write(record.getData());
		}

		if (!gzip) {
			return out.toByteArray();
		}

		ByteArrayOutputStream output = new ByteArrayOutputStream();
		try {
			GZIPOutputStream gzipOutputStream = new GZIPOutputStream(output);
			gzipOutputStream.write(out.toByteArray());
			gzipOutputStream.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return output.toByteArray();
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

		S3ObjectInputStream input = object.getObjectContent();
		ByteArrayOutputStream output = new ByteArrayOutputStream();

		try {
			if (gzip) {
				readGzipped(input, output);
			} else {
				read(input, output);
			}
			input.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return ByteBuffer.wrap(output.toByteArray());
	}

	private void read(S3ObjectInputStream input, ByteArrayOutputStream output) throws IOException {
		int size;
		byte[] tmp = new byte[BUFSIZE];
		while ((size = input.read(tmp)) != -1) {
			output.write(tmp, 0, size);
		}
	}

	private void readGzipped(S3ObjectInputStream input, ByteArrayOutputStream output) throws IOException {
		GZIPInputStream gzip = new GZIPInputStream(input);
		int size;
		byte[] tmp = new byte[BUFSIZE];
		while ((size = gzip.read(tmp)) != -1) {
			output.write(tmp, 0, size);
		}
		gzip.close();
	}
}
