package org.ds.chronos.aws;

import org.ds.chronos.api.Chronicle;
import org.ds.chronos.api.PartitionPeriod;
import org.ds.chronos.api.PartitionedChronicle;

import com.amazonaws.services.s3.AmazonS3Client;

public class S3ParitionedChronicle extends PartitionedChronicle {

	final AmazonS3Client client;
	final String bucket;

	public S3ParitionedChronicle(AmazonS3Client client, String bucket, String keyPrefix, PartitionPeriod period) {
		super(keyPrefix, period);
		this.client = client;
		this.bucket = bucket;
	}

	@Override
  protected Chronicle getPartition(String key) {
	  return new S3Chronicle(client, bucket, key);
  }

}
