package com.linkedin.gms.factory.aws;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.ObjectStorageConfiguration;
import com.linkedin.metadata.config.search.EmbeddingProviderConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.utils.SdkAutoCloseable;

/**
 * Process-wide AWS credential and object-storage client lifecycle for GMS.
 *
 * <p>Centralizes {@link DefaultCredentialsProvider#create()} and shared S3 clients so IRSA refresh
 * tasks are not orphaned by per-call client construction. Callers should inject these beans rather
 * than creating their own credential providers or S3 clients.
 */
@Slf4j
@Configuration
public class AwsClientFactory {

  private static final String OBJECT_STORAGE_SESSION_NAME = "object-storage-session";

  @Autowired(required = false)
  private ConfigurationProvider configurationProvider;

  @Nullable private DefaultCredentialsProvider defaultCredentialsProvider;
  @Nullable private StsAssumeRoleCredentialsProvider objectStorageRoleCredentialsProvider;
  @Nullable private S3Client managedObjectStorageS3Client;
  @Nullable private S3Presigner managedObjectStorageS3Presigner;

  /**
   * The only place in GMS that calls {@link DefaultCredentialsProvider#create()}.
   *
   * <p>Created when AWS region/endpoint configuration or Bedrock embedding is present.
   */
  @Bean(name = "defaultAwsCredentialsProvider")
  @Nullable
  protected AwsCredentialsProvider defaultAwsCredentialsProvider() {
    if (!isAwsCredentialsRequired()) {
      log.debug(
          "Skipping DefaultCredentialsProvider (no AWS region, endpoint, or Bedrock embedding configured)");
      return null;
    }
    log.info("Creating shared DefaultCredentialsProvider bean");
    defaultCredentialsProvider = DefaultCredentialsProvider.create();
    return defaultCredentialsProvider;
  }

  @Bean(name = "objectStorageCredentialsProvider")
  @Nullable
  protected AwsCredentialsProvider objectStorageCredentialsProvider(
      @Autowired(required = false) @Qualifier("defaultAwsCredentialsProvider")
          AwsCredentialsProvider defaultProvider,
      @Autowired(required = false) @Qualifier("stsClient") StsClient stsClient) {
    ObjectStorageConfiguration objectStorageConfiguration = getObjectStorageConfiguration();
    String roleArn =
        objectStorageConfiguration != null ? objectStorageConfiguration.getRoleArn() : null;
    if (roleArn == null || roleArn.trim().isEmpty()) {
      return defaultProvider;
    }
    if (stsClient == null) {
      throw new IllegalStateException(
          "StsClient bean is required when datahub.objectStorage.roleArn is configured");
    }
    log.info("Creating shared StsAssumeRoleCredentialsProvider for object storage");
    objectStorageRoleCredentialsProvider =
        StsAssumeRoleCredentialsProvider.builder()
            .stsClient(stsClient)
            .refreshRequest(r -> r.roleArn(roleArn).roleSessionName(OBJECT_STORAGE_SESSION_NAME))
            .asyncCredentialUpdateEnabled(true)
            .build();
    return objectStorageRoleCredentialsProvider;
  }

  @Bean(name = "objectStorageS3Client")
  @Nullable
  protected S3Client objectStorageS3Client(
      @Autowired(required = false) @Qualifier("objectStorageCredentialsProvider")
          AwsCredentialsProvider credentialsProvider) {
    ObjectStorageConfiguration objectStorageConfiguration = getObjectStorageConfiguration();
    String roleArn =
        objectStorageConfiguration != null ? objectStorageConfiguration.getRoleArn() : null;
    boolean hasRoleArn = roleArn != null && !roleArn.trim().isEmpty();

    String endpointUrl = envOrProperty("AWS_ENDPOINT_URL");
    String awsRegion = envOrProperty("AWS_REGION");
    String awsRegionProp = System.getProperty("aws.region");
    boolean hasAwsEndpoint = endpointUrl != null && !endpointUrl.isEmpty();
    boolean hasAwsRegion =
        (awsRegion != null && !awsRegion.trim().isEmpty())
            || (awsRegionProp != null && !awsRegionProp.trim().isEmpty());

    if (!hasRoleArn && !hasAwsEndpoint && !hasAwsRegion) {
      log.debug(
          "Skipping shared object storage S3Client (no roleArn, AWS_ENDPOINT_URL, AWS_REGION, or aws.region)");
      return null;
    }

    if (hasRoleArn && credentialsProvider == null) {
      return null;
    }

    try {
      var clientBuilder = S3Client.builder();
      if (credentialsProvider != null) {
        clientBuilder.credentialsProvider(credentialsProvider);
      }
      if (hasAwsEndpoint) {
        clientBuilder.endpointOverride(java.net.URI.create(endpointUrl));
        clientBuilder.forcePathStyle(true);
        if (!hasAwsRegion) {
          clientBuilder.region(Region.US_EAST_1);
        }
      }

      log.info("Creating shared object storage S3Client bean");
      managedObjectStorageS3Client = clientBuilder.build();
      return managedObjectStorageS3Client;
    } catch (Exception e) {
      log.error("Failed to create shared object storage S3Client", e);
      return null;
    }
  }

  @Bean(name = "objectStorageS3Presigner")
  @Nullable
  protected S3Presigner objectStorageS3Presigner(
      @Autowired(required = false) @Qualifier("objectStorageS3Client") S3Client s3Client) {
    if (s3Client == null) {
      return null;
    }
    managedObjectStorageS3Presigner = buildPresigner(s3Client);
    return managedObjectStorageS3Presigner;
  }

  @PreDestroy
  public void shutdown() {
    closeQuietly(managedObjectStorageS3Presigner);
    managedObjectStorageS3Presigner = null;
    closeQuietly(managedObjectStorageS3Client);
    managedObjectStorageS3Client = null;
    closeQuietly(objectStorageRoleCredentialsProvider);
    objectStorageRoleCredentialsProvider = null;
    closeQuietly(defaultCredentialsProvider);
    defaultCredentialsProvider = null;
  }

  @Nonnull
  private static S3Presigner buildPresigner(@Nonnull S3Client s3Client) {
    var presignerBuilder =
        S3Presigner.builder()
            .credentialsProvider(s3Client.serviceClientConfiguration().credentialsProvider())
            .region(s3Client.serviceClientConfiguration().region());

    String endpointUrl = envOrProperty("AWS_ENDPOINT_URL");
    if (endpointUrl != null && !endpointUrl.isEmpty()) {
      presignerBuilder.endpointOverride(java.net.URI.create(endpointUrl));
      presignerBuilder.serviceConfiguration(
          software.amazon.awssdk.services.s3.S3Configuration.builder()
              .pathStyleAccessEnabled(true)
              .build());
    }

    return presignerBuilder.build();
  }

  @Nullable
  private ObjectStorageConfiguration getObjectStorageConfiguration() {
    if (configurationProvider == null || configurationProvider.getDatahub() == null) {
      return null;
    }
    return configurationProvider.getDatahub().getObjectStorage();
  }

  static boolean isAwsConfigured() {
    return hasAwsEndpoint() || hasAwsRegion();
  }

  /** True when semantic search uses aws-bedrock and a target region is configured. */
  boolean isBedrockEmbeddingConfigured() {
    if (configurationProvider == null || configurationProvider.getElasticSearch() == null) {
      return false;
    }
    EntityIndexConfiguration entityIndex =
        configurationProvider.getElasticSearch().getEntityIndex();
    if (entityIndex == null) {
      return false;
    }
    SemanticSearchConfiguration semanticSearch = entityIndex.getSemanticSearch();
    if (semanticSearch == null || !semanticSearch.isEnabled()) {
      return false;
    }
    EmbeddingProviderConfiguration embeddingProvider = semanticSearch.getEmbeddingProvider();
    if (embeddingProvider == null || !"aws-bedrock".equalsIgnoreCase(embeddingProvider.getType())) {
      return false;
    }
    EmbeddingProviderConfiguration.BedrockConfig bedrock = embeddingProvider.getBedrock();
    return bedrock != null
        && bedrock.getAwsRegion() != null
        && !bedrock.getAwsRegion().trim().isEmpty();
  }

  private boolean isAwsCredentialsRequired() {
    return isAwsConfigured() || isBedrockEmbeddingConfigured();
  }

  private static boolean hasAwsEndpoint() {
    String endpointUrl = envOrProperty("AWS_ENDPOINT_URL");
    return endpointUrl != null && !endpointUrl.isEmpty();
  }

  private static boolean hasAwsRegion() {
    String awsRegion = envOrProperty("AWS_REGION");
    if (awsRegion != null && !awsRegion.trim().isEmpty()) {
      return true;
    }
    String awsRegionProp = System.getProperty("aws.region");
    return awsRegionProp != null && !awsRegionProp.trim().isEmpty();
  }

  @Nullable
  private static String envOrProperty(@Nonnull String name) {
    String value = System.getenv(name);
    if (value == null || value.isEmpty()) {
      value = System.getProperty(name);
    }
    return value;
  }

  private static void closeQuietly(@Nullable SdkAutoCloseable closeable) {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
    } catch (Exception e) {
      log.warn("Failed to close AWS client during shutdown", e);
    }
  }
}
