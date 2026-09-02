package com.linkedin.gms.factory.s3;

import com.linkedin.gms.factory.aws.AwsClientFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import jakarta.annotation.PreDestroy;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;

@Slf4j
@Configuration
@Import(AwsClientFactory.class)
public class StsClientFactory {

  @Autowired(required = false)
  @Qualifier("defaultAwsCredentialsProvider")
  private AwsCredentialsProvider defaultAwsCredentialsProvider;

  @Autowired(required = false)
  private ConfigurationProvider configurationProvider;

  @Nullable private StsClient managedStsClient;

  @Bean(name = "stsClient")
  @Nullable
  protected StsClient getInstance() {
    String endpointUrl = System.getenv("AWS_ENDPOINT_URL");
    if (endpointUrl == null || endpointUrl.isEmpty()) {
      endpointUrl = System.getProperty("AWS_ENDPOINT_URL");
    }
    String awsRegion = System.getenv("AWS_REGION");
    if (awsRegion == null || awsRegion.trim().isEmpty()) {
      awsRegion = System.getProperty("AWS_REGION");
    }
    String awsRegionProp = System.getProperty("aws.region");

    boolean hasAwsEndpoint = endpointUrl != null && !endpointUrl.isEmpty();
    boolean hasAwsRegion =
        (awsRegion != null && !awsRegion.trim().isEmpty())
            || (awsRegionProp != null && !awsRegionProp.trim().isEmpty());
    boolean hasObjectStorageRoleArn =
        AwsClientFactory.isObjectStorageRoleArnConfigured(configurationProvider);

    if (!hasAwsEndpoint && !hasAwsRegion && !hasObjectStorageRoleArn) {
      log.debug(
          "Skipping STS client creation (no AWS_ENDPOINT_URL, AWS_REGION, aws.region, or objectStorage.roleArn)");
      return null;
    }

    log.info("Creating StsClient bean");

    try {
      var clientBuilder = StsClient.builder();

      if (hasAwsEndpoint) {
        log.info("Configuring StsClient with custom endpoint: {}", endpointUrl);
        clientBuilder.endpointOverride(java.net.URI.create(endpointUrl));

        log.info("Using dummy credentials for LocalStack/custom endpoint");
        clientBuilder.credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")));

        clientBuilder.region(Region.US_EAST_1);
      } else if (defaultAwsCredentialsProvider != null) {
        log.info("Using shared DefaultCredentialsProvider for StsClient");
        clientBuilder.credentialsProvider(defaultAwsCredentialsProvider);
        // When only roleArn is configured, leave region to the SDK default chain (IRSA / IMDS /
        // AWS_REGION). Explicit region/endpoint paths set region above or via the environment.
      }

      managedStsClient = clientBuilder.build();
      log.info("Successfully created StsClient");
      return managedStsClient;

    } catch (Exception e) {
      String msg = e.getMessage();
      boolean expectedNonAws =
          e instanceof SdkClientException
              && msg != null
              && (msg.contains("Unable to load region")
                  || msg.contains("EC2 metadata service")
                  || msg.contains("Unable to load credentials"));
      if (expectedNonAws) {
        log.debug("STS client not available (not running in AWS or AWS not configured): {}", msg);
      } else {
        log.error("Failed to create STS client", e);
      }
      return null;
    }
  }

  @PreDestroy
  public void shutdown() {
    if (managedStsClient != null) {
      try {
        managedStsClient.close();
      } catch (Exception e) {
        log.warn("Failed to close StsClient during shutdown", e);
      }
      managedStsClient = null;
    }
  }
}
