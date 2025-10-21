package com.linkedin.gms.factory.s3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;

@Slf4j
@Configuration
public class StsClientFactory {

  @Bean(name = "stsClient")
  protected StsClient getInstance() {
    log.info("Creating StsClient bean");

    try {
      var clientBuilder = StsClient.builder();

      // Configure endpoint URL if provided (for LocalStack or custom endpoints)
      String endpointUrl = System.getenv("AWS_ENDPOINT_URL");
      if (endpointUrl != null && !endpointUrl.isEmpty()) {
        log.info("Configuring StsClient with custom endpoint: {}", endpointUrl);
        clientBuilder.endpointOverride(java.net.URI.create(endpointUrl));

        // For LocalStack, use dummy credentials
        log.info("Using dummy credentials for LocalStack/custom endpoint");
        clientBuilder.credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")));

        // Set a default region for LocalStack
        clientBuilder.region(Region.US_EAST_1);
      } else {
        log.info("Using default AWS STS configuration");
        // Let AWS SDK use default credential provider chain and region
      }

      StsClient client = clientBuilder.build();
      log.info("Successfully created StsClient");
      return client;

    } catch (Exception e) {
      log.error("Failed to create STS client", e);
      return null;
    }
  }
}
