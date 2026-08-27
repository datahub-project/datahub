package com.linkedin.gms.factory.aws;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.ObjectStorageConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
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
import software.amazon.awssdk.core.exception.SdkClientException;
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
 *
 * <p>Non-AWS environments (no region/endpoint, no IAM auth, no Bedrock, no object-storage role)
 * skip bean creation and do not fail startup.
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
   * <p>Created when AWS region/endpoint, OpenSearch IAM auth, Bedrock embedding, or object-storage
   * role assumption is configured.
   */
  @Bean(name = "defaultAwsCredentialsProvider")
  @Nullable
  protected AwsCredentialsProvider defaultAwsCredentialsProvider() {
    if (!isAwsCredentialsRequired()) {
      log.debug(
          "Skipping DefaultCredentialsProvider (no AWS region/endpoint, OpenSearch IAM, Bedrock, or object-storage roleArn)");
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
    String roleArn = getObjectStorageRoleArn();
    if (roleArn == null) {
      return defaultProvider;
    }
    if (stsClient == null) {
      // Soft-skip so search-only / non-AWS processes that import this factory still start.
      // objectStorageS3Client fails fast when roleArn is set with AWS region/endpoint present.
      log.warn(
          "datahub.objectStorage.roleArn is set but StsClient is unavailable; skipping assume-role credentials");
      return null;
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
    String roleArn = getObjectStorageRoleArn();
    boolean hasRoleArn = roleArn != null;

    String endpointUrl = envOrProperty("AWS_ENDPOINT_URL");
    boolean hasAwsEndpoint = endpointUrl != null && !endpointUrl.isEmpty();
    boolean hasAwsRegion = hasAwsRegion();

    if (!hasRoleArn && !hasAwsEndpoint && !hasAwsRegion) {
      log.debug(
          "Skipping shared object storage S3Client (no roleArn, AWS_ENDPOINT_URL, AWS_REGION, or aws.region)");
      return null;
    }

    if (hasRoleArn && credentialsProvider == null) {
      // Soft-skip so search-only contexts that import AwsClientFactory without StsClientFactory
      // (e.g. MAE) still start when DATAHUB_ROLE_ARN is present alongside AWS_REGION.
      log.warn(
          "datahub.objectStorage.roleArn is set but assume-role credentials are unavailable; "
              + "skipping shared S3Client (object storage will be unavailable until StsClient is wired)");
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
      // Explicit role assumption misconfiguration should fail fast. Opportunistic
      // region/endpoint-only setup (including invalid LocalStack URLs in tests / non-AWS)
      // soft-skips.
      if (hasRoleArn) {
        throw new IllegalStateException("Failed to create shared object storage S3Client", e);
      }
      if (isExpectedNonAwsFailure(e) || isInvalidEndpointConfiguration(e)) {
        log.debug(
            "Skipping shared object storage S3Client (AWS SDK not usable in this environment): {}",
            e.getMessage());
        return null;
      }
      throw new IllegalStateException("Failed to create shared object storage S3Client", e);
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
    return resolveObjectStorageConfiguration(configurationProvider);
  }

  @Nullable
  private String getObjectStorageRoleArn() {
    return resolveObjectStorageRoleArn(configurationProvider);
  }

  /**
   * Shared object-storage roleArn lookup for factories that need to know whether STS/S3 should be
   * created for assume-role.
   */
  @Nullable
  public static String resolveObjectStorageRoleArn(
      @Nullable ConfigurationProvider configurationProvider) {
    ObjectStorageConfiguration objectStorageConfiguration =
        resolveObjectStorageConfiguration(configurationProvider);
    if (objectStorageConfiguration == null) {
      return null;
    }
    String roleArn = objectStorageConfiguration.getRoleArn();
    if (roleArn == null || roleArn.trim().isEmpty()) {
      return null;
    }
    return roleArn.trim();
  }

  public static boolean isObjectStorageRoleArnConfigured(
      @Nullable ConfigurationProvider configurationProvider) {
    return resolveObjectStorageRoleArn(configurationProvider) != null;
  }

  @Nullable
  private static ObjectStorageConfiguration resolveObjectStorageConfiguration(
      @Nullable ConfigurationProvider configurationProvider) {
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

  /** True when OpenSearch requests are signed with AWS IAM. */
  boolean isOpenSearchIamAuthConfigured() {
    if (configurationProvider == null || configurationProvider.getElasticSearch() == null) {
      return false;
    }
    ElasticSearchConfiguration esConfig = configurationProvider.getElasticSearch();
    return esConfig.isOpensearchUseAwsIamAuth();
  }

  /** True when object storage is configured to assume an IAM role. */
  boolean isObjectStorageRoleArnConfigured() {
    return isObjectStorageRoleArnConfigured(configurationProvider);
  }

  boolean isAwsCredentialsRequired() {
    return isAwsConfigured()
        || isBedrockEmbeddingConfigured()
        || isOpenSearchIamAuthConfigured()
        || isObjectStorageRoleArnConfigured();
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

  static boolean isExpectedNonAwsFailure(@Nonnull Throwable error) {
    Throwable current = error;
    while (current != null) {
      if (current instanceof SdkClientException) {
        String msg = current.getMessage();
        if (msg != null
            && (msg.contains("Unable to load region")
                || msg.contains("EC2 metadata service")
                || msg.contains("Unable to load credentials"))) {
          return true;
        }
      }
      current = current.getCause();
    }
    return false;
  }

  static boolean isInvalidEndpointConfiguration(@Nonnull Throwable error) {
    Throwable current = error;
    while (current != null) {
      if (current instanceof IllegalArgumentException
          || current instanceof java.net.URISyntaxException) {
        String msg = current.getMessage();
        if (msg != null
            && (msg.contains("URI")
                || msg.contains("uri")
                || msg.contains("Illegal character")
                || msg.contains("endpoint"))) {
          return true;
        }
      }
      current = current.getCause();
    }
    return false;
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
