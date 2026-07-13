package com.linkedin.gms.factory.objectstorage;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.linkedin.gms.factory.config.ConfigurationProvider;
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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

@Slf4j
@Configuration
public class ObjectStorageClientFactory {

  @Autowired private ConfigurationProvider configurationProvider;

  @Autowired(required = false)
  private StsClient stsClient;

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

      ObjectStorageLocation resolvedLocation = location.get();
      int multipartThreshold =
          objectStorageConfiguration != null
                  && objectStorageConfiguration.getMultipartThresholdBytes() != null
              ? objectStorageConfiguration.getMultipartThresholdBytes()
              : S3ObjectStorageClient.DEFAULT_MULTIPART_THRESHOLD_BYTES;
      int multipartPartSize =
          objectStorageConfiguration != null
                  && objectStorageConfiguration.getMultipartPartSizeBytes() != null
              ? objectStorageConfiguration.getMultipartPartSizeBytes()
              : S3ObjectStorageClient.DEFAULT_MULTIPART_PART_SIZE_BYTES;

      return switch (resolvedLocation.provider()) {
        case LOCAL -> new LocalObjectStorageClient(resolvedLocation.localRoot());
        case S3 -> {
          S3Client s3Client = createS3Client(objectStorageConfiguration);
          if (s3Client == null) {
            yield null;
          }
          yield new S3ObjectStorageClient(
              s3Client,
              resolvedLocation.bucket(),
              emptyToNull(resolvedLocation.keyPrefix()),
              multipartThreshold,
              multipartPartSize);
        }
        case GCS -> {
          Storage storage = StorageOptions.getDefaultInstance().getService();
          yield new GcsObjectStorageClient(
              storage,
              resolvedLocation.bucket(),
              emptyToNull(resolvedLocation.keyPrefix()),
              multipartThreshold,
              multipartPartSize);
        }
      };
    } catch (Exception e) {
      log.error("Failed to create ObjectStorageClient", e);
      return null;
    }
  }

  @Nullable
  private S3Client createS3Client(@Nullable ObjectStorageConfiguration objectStorageConfiguration) {
    String roleArn =
        objectStorageConfiguration != null ? objectStorageConfiguration.getRoleArn() : null;
    if (roleArn != null && !roleArn.trim().isEmpty()) {
      if (stsClient == null) {
        throw new IllegalStateException(
            "StsClient bean is required when datahub.objectStorage.roleArn is configured");
      }
      StsAssumeRoleCredentialsProvider credentialsProvider =
          StsAssumeRoleCredentialsProvider.builder()
              .stsClient(stsClient)
              .refreshRequest(r -> r.roleArn(roleArn).roleSessionName("object-storage-session"))
              .asyncCredentialUpdateEnabled(true)
              .build();
      return buildS3ClientBuilder().credentialsProvider(credentialsProvider).build();
    }

    String endpointUrl = envOrProperty("AWS_ENDPOINT_URL");
    String awsRegion = envOrProperty("AWS_REGION");
    String awsRegionProp = System.getProperty("aws.region");
    boolean hasAwsEndpoint = endpointUrl != null && !endpointUrl.isEmpty();
    boolean hasAwsRegion =
        (awsRegion != null && !awsRegion.trim().isEmpty())
            || (awsRegionProp != null && !awsRegionProp.trim().isEmpty());

    if (!hasAwsEndpoint && !hasAwsRegion) {
      log.debug(
          "Skipping S3 ObjectStorageClient (no roleArn, AWS_ENDPOINT_URL, AWS_REGION, or aws.region)");
      return null;
    }

    var clientBuilder = buildS3ClientBuilder();
    if (hasAwsEndpoint) {
      clientBuilder.endpointOverride(java.net.URI.create(endpointUrl));
      clientBuilder.forcePathStyle(true);
      if (!hasAwsRegion) {
        clientBuilder.region(Region.US_EAST_1);
      }
    }
    return clientBuilder.build();
  }

  private software.amazon.awssdk.services.s3.S3ClientBuilder buildS3ClientBuilder() {
    return S3Client.builder();
  }

  @Nullable
  private static String emptyToNull(@Nullable String value) {
    if (value == null || value.isEmpty()) {
      return null;
    }
    return value;
  }

  @Nullable
  private static String envOrProperty(@Nonnull String name) {
    String value = System.getenv(name);
    if (value == null || value.isEmpty()) {
      value = System.getProperty(name);
    }
    return value;
  }
}
