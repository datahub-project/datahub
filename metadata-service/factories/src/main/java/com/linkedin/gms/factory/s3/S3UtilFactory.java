package com.linkedin.gms.factory.s3;

import com.linkedin.metadata.utils.aws.S3Util;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

@Slf4j
@Configuration
public class S3UtilFactory {

  @Autowired(required = false)
  private StsClient stsClient;

  @Value("${datahub.s3.roleArn:#{null}}")
  private String roleArn;

  @Bean(name = "s3Util")
  @Nullable
  protected S3Util getInstance() {
    try {
      if (roleArn != null && !roleArn.trim().isEmpty()) {
        log.info("Using STS role-based S3Util with role ARN: {}", roleArn);
        if (stsClient == null) {
          throw new IllegalStateException(
              "StsClient bean is required when roleArn is configured. "
                  + "Ensure StsClientFactory is properly configured.");
        }
        return new S3Util(stsClient, roleArn);
      } else {
        log.info("Using default S3Util with default credentials");
        var clientBuilder = S3Client.builder();

        // Configure endpoint URL if provided (for LocalStack or custom S3 endpoints)
        String endpointUrl = System.getenv("AWS_ENDPOINT_URL");
        if (endpointUrl != null && !endpointUrl.isEmpty()) {
          log.info("Configuring S3Client with custom endpoint: {}", endpointUrl);
          clientBuilder.endpointOverride(java.net.URI.create(endpointUrl));
          clientBuilder.forcePathStyle(true);
        }

        S3Client s3Client = clientBuilder.build();
        return new S3Util(s3Client);
      }
    } catch (Exception e) {
      log.error("Failed to create S3Utils", e);
      return null;
    }
  }
}
