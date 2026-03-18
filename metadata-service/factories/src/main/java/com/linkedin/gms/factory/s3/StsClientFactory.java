package com.linkedin.gms.factory.s3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;

@Slf4j
@Configuration
public class StsClientFactory {

  @Bean(name = "stsClient")
  protected StsClient getInstance() {
    // Env takes precedence; system properties allow overrides (e.g. tests in/outside AWS)
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

    if (!hasAwsEndpoint && !hasAwsRegion) {
      log.debug(
          "Skipping STS client creation (no AWS_ENDPOINT_URL, AWS_REGION, or aws.region set)");
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
      } else {
        log.info("Using default AWS STS configuration");
      }

      StsClient client = clientBuilder.build();
      log.info("Successfully created StsClient");
      return client;

    } catch (Exception e) {
      String msg = e.getMessage();
      boolean expectedNonAws =
          e instanceof SdkClientException
              && msg != null
              && (msg.contains("Unable to load region") || msg.contains("EC2 metadata service"));
      if (expectedNonAws) {
        log.debug("STS client not available (not running in AWS or AWS not configured): {}", msg);
      } else {
        log.error("Failed to create STS client", e);
      }
      return null;
    }
  }
}
