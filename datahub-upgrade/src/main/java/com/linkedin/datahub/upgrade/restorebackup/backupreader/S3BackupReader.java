package com.linkedin.datahub.upgrade.restorebackup.backupreader;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.linkedin.datahub.upgrade.UpgradeContext;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;


@Slf4j
public class S3BackupReader implements BackupReader {
  private final AmazonS3 _client;

  private static final String PARQUET_SUFFIX = ".gz.parquet";
  private static final String TEMP_FILE_PATH = "/tmp/backup.gz.parquet";

  public S3BackupReader() {
    _client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
    // Need below to solve issue with hadoop path class not working in linux systems
    // https://stackoverflow.com/questions/41864985/hadoop-ioexception-failure-to-login
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("hduser"));
  }

  @Override
  public String getName() {
    return "S3_PARQUET";
  }

  @Nonnull
  @Override
  public EbeanAspectBackupIterator getBackupIterator(UpgradeContext context) {
    Optional<String> bucket = context.parsedArgs().get("BACKUP_S3_BUCKET");
    Optional<String> path = context.parsedArgs().get("BACKUP_S3_PATH");
    if (!bucket.isPresent() || !path.isPresent()) {
      throw new IllegalArgumentException(
          "BACKUP_S3_BUCKET and BACKUP_S3_PATH must be set to run RestoreBackup through S3");
    }
    Optional<String> localFilePath = getFileKey(bucket.get(), path.get()).flatMap(key -> saveFile(bucket.get(), key));
    if (!localFilePath.isPresent()) {
      throw new RuntimeException(
          String.format("Backup file on path %s in bucket %s is not found", path.get(), bucket.get()));
    }
    try {
      ParquetReader<GenericRecord> reader =
          AvroParquetReader.<GenericRecord>builder(new Path(localFilePath.get())).build();
      return new ParquetEbeanAspectBackupIterator(reader);
    } catch (IOException e) {
      throw new RuntimeException("Failed to build ParquetReader");
    }
  }

  private Optional<String> getFileKey(String bucket, String path) {
    ListObjectsV2Result objectListResult = _client.listObjectsV2(bucket, path);
    return objectListResult.getObjectSummaries()
        .stream()
        .filter(os -> os.getKey().endsWith(PARQUET_SUFFIX))
        .findFirst()
        .map(S3ObjectSummary::getKey);
  }

  private Optional<String> saveFile(String bucket, String key) {
    System.out.format("Downloading %s from S3 bucket %s...\n", key, bucket);
    final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.DEFAULT_REGION).build();
    try (S3Object o = s3.getObject(bucket, key);
        S3ObjectInputStream s3is = o.getObjectContent();
        FileOutputStream fos = FileUtils.openOutputStream(new File(TEMP_FILE_PATH))) {
      byte[] readBuf = new byte[1024];
      int readLen = 0;
      while ((readLen = s3is.read(readBuf)) > 0) {
        fos.write(readBuf, 0, readLen);
      }
      return Optional.of(TEMP_FILE_PATH);
    } catch (AmazonServiceException e) {
      System.err.println(e.getErrorMessage());
      return Optional.empty();
    } catch (IOException e) {
      System.err.println(e.getMessage());
      return Optional.empty();
    }
  }
}
