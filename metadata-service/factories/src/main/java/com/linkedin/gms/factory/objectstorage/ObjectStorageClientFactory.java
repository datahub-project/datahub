package com.linkedin.gms.factory.objectstorage;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.linkedin.gms.factory.aws.AwsClientFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.s3.StsClientFactory;
import com.linkedin.metadata.config.ObjectStorageConfiguration;
import com.linkedin.metadata.utils.objectstorage.GcsObjectStorageClient;
import com.linkedin.metadata.utils.objectstorage.LocalObjectStorageClient;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageClient;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageLocation;
import com.linkedin.metadata.utils.objectstorage.S3ObjectStorageClient;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

@Slf4j
@Configuration
@Import({AwsClientFactory.class, StsClientFactory.class})
public class ObjectStorageClientFactory {

  @Autowired private ConfigurationProvider configurationProvider;

  @Autowired(required = false)
  @Qualifier("objectStorageS3Client")
  private S3Client objectStorageS3Client;

  @Autowired(required = false)
  @Qualifier("objectStorageS3Presigner")
  private S3Presigner objectStorageS3Presigner;

  @Bean(name = "objectStorageClient")
  @Nullable
  protected ObjectStorageClient getInstance() {
    try {
      ObjectStorageConfiguration objectStorageConfiguration =
          configurationProvider.getDatahub().getObjectStorage();

      String legacyBucketName =
          objectStorageConfiguration != null ? objectStorageConfiguration.getBucket() : null;
      String configuredUri =
          objectStorageConfiguration != null ? objectStorageConfiguration.getUri() : null;
      String legacyPath =
          objectStorageConfiguration != null ? objectStorageConfiguration.getPath() : null;
      String legacyProvider =
          objectStorageConfiguration != null ? objectStorageConfiguration.getProvider() : null;

      Optional<ObjectStorageLocation> location =
          ObjectStorageLocation.resolve(
              configuredUri, legacyBucketName, legacyPath, legacyProvider);
      if (location.isEmpty()) {
        log.debug("Skipping ObjectStorageClient creation (no object storage location configured)");
        return null;
      }

      return clientFor(location.get());
    } catch (Exception e) {
      log.error("Failed to create ObjectStorageClient", e);
      return null;
    }
  }

  /**
   * Builds a client rooted at an arbitrary location rather than only the configured one, so a
   * caller addressing a different bucket reuses this credential resolution (role assumption,
   * endpoint override, region) and this provider routing instead of standing up its own.
   * Credentials and multipart sizing still come from {@code datahub.objectStorage}; only the
   * location varies.
   *
   * <p>Null when the provider needs a client that cannot be built — today only S3, when AWS is
   * unconfigured.
   */
  @Nullable
  public ObjectStorageClient clientFor(@Nonnull ObjectStorageLocation location) {
    ObjectStorageConfiguration config = configurationProvider.getDatahub().getObjectStorage();
    int multipartThreshold =
        config != null && config.getMultipartThresholdBytes() != null
            ? config.getMultipartThresholdBytes()
            : S3ObjectStorageClient.DEFAULT_MULTIPART_THRESHOLD_BYTES;
    int multipartPartSize =
        config != null && config.getMultipartPartSizeBytes() != null
            ? config.getMultipartPartSizeBytes()
            : S3ObjectStorageClient.DEFAULT_MULTIPART_PART_SIZE_BYTES;

    return switch (location.provider()) {
      case LOCAL -> new LocalObjectStorageClient(location.localRoot());
      case S3 -> {
        if (objectStorageS3Client == null || objectStorageS3Presigner == null) {
          yield null;
        }
        yield new S3ObjectStorageClient(
            objectStorageS3Client,
            objectStorageS3Presigner,
            location.bucket(),
            emptyToNull(location.keyPrefix()),
            multipartThreshold,
            multipartPartSize);
      }
      case GCS -> {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        yield new GcsObjectStorageClient(
            storage,
            location.bucket(),
            emptyToNull(location.keyPrefix()),
            multipartThreshold,
            multipartPartSize);
      }
    };
  }

  @Nullable
  private static String emptyToNull(@Nullable String value) {
    if (value == null || value.isEmpty()) {
      return null;
    }
    return value;
  }
}
