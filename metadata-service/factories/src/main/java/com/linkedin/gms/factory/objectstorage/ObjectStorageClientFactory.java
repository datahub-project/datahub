package com.linkedin.gms.factory.objectstorage;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.ObjectStorageConfiguration;
import com.linkedin.metadata.config.S3Configuration;
import com.linkedin.metadata.utils.objectstorage.GcsObjectStorageClient;
import com.linkedin.metadata.utils.objectstorage.LocalObjectStorageClient;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageClient;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageProvider;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageProviderResolver;
import com.linkedin.metadata.utils.objectstorage.S3ObjectStorageClient;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
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
      S3Configuration s3Configuration = configurationProvider.getDatahub().getS3();
      ObjectStorageConfiguration objectStorageConfiguration =
          configurationProvider.getDatahub().getObjectStorage();

      String bucketName = s3Configuration != null ? s3Configuration.getBucketName() : null;
      String path =
          objectStorageConfiguration != null ? objectStorageConfiguration.getPath() : null;
      String providerHint =
          objectStorageConfiguration != null ? objectStorageConfiguration.getProvider() : null;

      ObjectStorageProvider provider =
          ObjectStorageProviderResolver.resolve(providerHint, bucketName);

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

      return switch (provider) {
        case LOCAL -> {
          LocalObjectStorageClient client = new LocalObjectStorageClient(path);
          if (!client.isConfigured()) {
            log.debug(
                "Skipping ObjectStorageClient creation (LOCAL provider requires non-empty path)");
            yield null;
          }
          yield client;
        }
        case S3 -> {
          if (bucketName == null || bucketName.isBlank()) {
            log.debug("Skipping ObjectStorageClient creation (S3 provider requires bucket name)");
            yield null;
          }
          S3Client s3Client = createS3Client(s3Configuration);
          if (s3Client == null) {
            yield null;
          }
          yield new S3ObjectStorageClient(
              s3Client, bucketName, path, multipartThreshold, multipartPartSize);
        }
        case GCS -> {
          if (bucketName == null || bucketName.isBlank()) {
            log.debug("Skipping ObjectStorageClient creation (GCS provider requires bucket name)");
            yield null;
          }
          Storage storage = StorageOptions.getDefaultInstance().getService();
          yield new GcsObjectStorageClient(
              storage, bucketName, path, multipartThreshold, multipartPartSize);
        }
      };
    } catch (Exception e) {
      log.error("Failed to create ObjectStorageClient", e);
      return null;
    }
  }

  @Nullable
  private S3Client createS3Client(@Nullable S3Configuration s3Configuration) {
    String roleArn = s3Configuration != null ? s3Configuration.getRoleArn() : null;
    if (roleArn != null && !roleArn.trim().isEmpty()) {
      if (stsClient == null) {
        throw new IllegalStateException(
            "StsClient bean is required when datahub.s3.roleArn is configured");
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
  private static String envOrProperty(@Nonnull String name) {
    String value = System.getenv(name);
    if (value == null || value.isEmpty()) {
      value = System.getProperty(name);
    }
    return value;
  }
}
