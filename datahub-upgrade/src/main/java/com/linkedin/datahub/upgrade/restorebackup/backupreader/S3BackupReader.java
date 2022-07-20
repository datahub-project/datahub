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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
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

  public static final String READER_NAME = "S3_PARQUET";
  public static final String S3_REGION = "S3_REGION";

  private final AmazonS3 _client;

  private static final String TEMP_DIR = "/tmp/";

  public S3BackupReader(@Nonnull List<Optional<String>> args) {
    if (args.size() != argNames().size()) {
      throw new IllegalArgumentException("Incorrect number of arguments for S3BackupReader.");
    }
    Regions region;
    String s3Region;
    Optional<String> arg = args.get(0);
    if (!arg.isPresent()) {
      log.warn("Region not provided, defaulting to us-west-2");
      s3Region = Regions.US_WEST_2.getName();
    } else {
      s3Region = arg.get();
    }
    try {
      region = Regions.fromName(s3Region);
    } catch (Exception e) {
      log.warn("Invalid region: {}, defaulting to us-west-2", s3Region);
      region = Regions.US_WEST_2;
    }
    _client = AmazonS3ClientBuilder.standard().withRegion(region).build();
    // Need below to solve issue with hadoop path class not working in linux systems
    // https://stackoverflow.com/questions/41864985/hadoop-ioexception-failure-to-login
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("hduser"));
  }

  public static List<String> argNames() {
    return Collections.singletonList(S3_REGION);
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
    List<String> s3Keys = getFileKey(bucket.get(), path.get());
    List<String> localFilePaths = s3Keys.stream()
        .map(key -> saveFile(bucket.get(), key))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
    if (localFilePaths.isEmpty()) {
      throw new RuntimeException(
          String.format("Backup file on path %s in bucket %s is not found", path.get(), bucket.get()));
    }
    List<ParquetReader<GenericRecord>> readers = localFilePaths.stream().map(filePath -> {
      try {
        return AvroParquetReader.<GenericRecord>builder(new Path(filePath)).build();
      } catch (IOException e) {
        throw new RuntimeException("Failed to build ParquetReader");
      }
    }).collect(Collectors.toList());
    return new ParquetEbeanAspectBackupIterator(readers);
  }

  private List<String> getFileKey(String bucket, String path) {
    ListObjectsV2Result objectListResult = _client.listObjectsV2(bucket, path);
    return objectListResult.getObjectSummaries()
        .stream()
        .map(S3ObjectSummary::getKey)
        .collect(Collectors.toList());
  }

  private Optional<String> saveFile(String bucket, String key) {
    log.info("Downloading {} from S3 bucket {}...", key, bucket);
    String[] path = key.split("/");
    String localFilePath;
    if (path.length > 0) {
      localFilePath = TEMP_DIR + path[path.length - 1];
    } else {
      localFilePath = "backup.gz.parquet";
    }

    try (S3Object o = _client.getObject(bucket, key);
        S3ObjectInputStream s3is = o.getObjectContent();
        FileOutputStream fos = FileUtils.openOutputStream(new File(localFilePath))) {
      byte[] readBuf = new byte[1024];
      int readLen = 0;
      while ((readLen = s3is.read(readBuf)) > 0) {
        fos.write(readBuf, 0, readLen);
      }
      return Optional.of(localFilePath);
    } catch (AmazonServiceException e) {
      System.err.println(e.getErrorMessage());
      return Optional.empty();
    } catch (IOException e) {
      System.err.println(e.getMessage());
      return Optional.empty();
    }
  }
}
