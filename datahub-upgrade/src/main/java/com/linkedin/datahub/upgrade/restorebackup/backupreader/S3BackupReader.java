package com.linkedin.datahub.upgrade.restorebackup.backupreader;

import com.linkedin.datahub.upgrade.UpgradeContext;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.avro.AvroParquetReader;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

@Slf4j
public class S3BackupReader implements BackupReader<ParquetReaderWrapper> {

  public static final String READER_NAME = "S3_PARQUET";
  public static final String BACKUP_S3_BUCKET = "BACKUP_S3_BUCKET";
  public static final String BACKUP_S3_PATH = "BACKUP_S3_PATH";
  public static final String S3_REGION = "S3_REGION";
  public static final String DOWNLOAD_POOL_SIZE = "DOWNLOAD_POOL_SIZE";
  private static final int DEFAULT_DOWNLOAD_POOL_SIZE = 20;

  private final S3Client _client;

  private static final String TEMP_DIR = "/tmp/";
  private final ExecutorService downloaderThreadPool;

  public S3BackupReader(@Nonnull List<Optional<String>> args) {
    if (args.size() != argNames().size()) {
      throw new IllegalArgumentException("Incorrect number of arguments for S3BackupReader.");
    }
    Region region;
    String s3Region;
    String arg = args.get(0).get();
    int downloadPoolSize = DEFAULT_DOWNLOAD_POOL_SIZE;
    String envDownloadPoolSize = System.getenv(DOWNLOAD_POOL_SIZE);
    if (envDownloadPoolSize != null) {
      try {
        downloadPoolSize = Integer.parseInt(envDownloadPoolSize);
      } catch (Exception e) {
        log.warn("DOWNLOAD_POOL_SIZE improperly set, falling back to default.");
      }
    }
    downloaderThreadPool = Executors.newFixedThreadPool(downloadPoolSize);

    if (arg == null) {
      log.warn("Region not provided, defaulting to us-west-2");
      s3Region = Region.US_WEST_2.id();
    } else {
      s3Region = arg;
    }
    try {
      region = Region.of(s3Region);
    } catch (Exception e) {
      log.warn("Invalid region: {}, defaulting to us-west-2", s3Region);
      region = Region.of(Region.US_WEST_2.id());
    }

    System.setProperty(
        "software.amazon.awssdk.http.service.impl",
        "software.amazon.awssdk.http.apache.ApacheSdkHttpService");

    _client =
        S3Client.builder()
            .region(region)
            .credentialsProvider(WebIdentityTokenFileCredentialsProvider.create())
            .build();
    // Need below to solve issue with hadoop path class not working in linux systems
    // https://stackoverflow.com/questions/41864985/hadoop-ioexception-failure-to-login
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("hduser"));
  }

  public static List<String> argNames() {
    return Collections.singletonList(S3_REGION);
  }

  @Override
  public String getName() {
    return READER_NAME;
  }

  @Nonnull
  @Override
  public EbeanAspectBackupIterator<ParquetReaderWrapper> getBackupIterator(UpgradeContext context) {
    long downloadStartTime = System.currentTimeMillis();
    String bucket = System.getenv(BACKUP_S3_BUCKET);
    String path = System.getenv(BACKUP_S3_PATH);
    if (bucket == null || path == null) {
      throw new IllegalArgumentException(
          "BACKUP_S3_BUCKET and BACKUP_S3_PATH must be set to run RestoreBackup through S3");
    }
    List<String> s3Keys = getFileKey(bucket, path);
    List<Future<Optional<String>>> s3Downloads =
        s3Keys.stream()
            .filter(key -> !key.endsWith("_SUCCESS"))
            .map(key -> this.downloaderThreadPool.submit(() -> saveFile(bucket, key)))
            .collect(Collectors.toList());

    final List<String> localFiles =
        s3Downloads.stream()
            .map(
                key -> {
                  try {
                    return key.get();
                  } catch (InterruptedException | ExecutionException e) {
                    return Optional.<String>empty();
                  }
                })
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
    long downloadEndTime = System.currentTimeMillis();
    log.info("Download time: {} milli-seconds", downloadEndTime - downloadStartTime);
    final List<ParquetReaderWrapper> readers =
        localFiles.stream()
            .map(
                filePath -> {
                  try {
                    // Try to read a record, only way to check if it is indeed a Parquet file
                    AvroParquetReader.<GenericRecord>builder(new Path(filePath)).build().read();
                    return new ParquetReaderWrapper(
                        AvroParquetReader.<GenericRecord>builder(new Path(filePath)).build(),
                        filePath);
                  } catch (IOException e) {
                    log.warn(
                        "Unable to read {} as parquet, this may or may not be important.",
                        filePath);
                    return null;
                  } catch (RuntimeException e) {
                    log.warn(
                        "Unable to read {} as parquet, this may or may not be important: {}",
                        filePath,
                        e.getCause());
                    return null;
                  }
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    if (readers.isEmpty()) {
      log.error(
          "No backup files on path {} in bucket {} were found. Did you mis-configure something?",
          path,
          bucket);
    }

    return new EbeanAspectBackupIterator<>(readers);
  }

  private List<String> getFileKey(String bucket, String path) {
    ListObjectsV2Request request =
        ListObjectsV2Request.builder().bucket(bucket).prefix(path).build();
    ListObjectsV2Iterable objectListResult = _client.listObjectsV2Paginator(request);
    return objectListResult.contents().stream().map(S3Object::key).collect(Collectors.toList());
  }

  private Optional<String> saveFile(String bucket, String key) {
    log.info("Downloading {} from S3 bucket {}...", key, bucket);
    String localFilePath;
    if (key.contains("/")) {
      String localFileName = key.replace("/", "_");
      localFileName = localFileName.replace("=", "_");
      localFilePath = TEMP_DIR + localFileName;
    } else {
      localFilePath = "backup.gz.parquet";
    }

    try (ResponseInputStream<GetObjectResponse> o =
            _client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build());
        FileOutputStream fos = FileUtils.openOutputStream(new File(localFilePath))) {
      byte[] readBuf = new byte[1024];
      int readLen = 0;
      while ((readLen = o.read(readBuf)) > 0) {
        fos.write(readBuf, 0, readLen);
      }
      return Optional.of(localFilePath);
    } catch (AwsServiceException | IOException e) {
      log.error(e.getMessage());
      return Optional.empty();
    }
  }
}
