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
  private final AmazonS3 _client;

  private static final String PARQUET_SUFFIX = ".gz.parquet";
  private static final String TEMP_DIR = "/tmp/";

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
        .filter(os -> os.getKey().endsWith(PARQUET_SUFFIX))
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

    final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.DEFAULT_REGION).build();
    try (S3Object o = s3.getObject(bucket, key);
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
