package com.linkedin.gms.factory.s3;

import com.linkedin.metadata.utils.aws.S3Util;
import javax.annotation.Nonnull;
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
  protected S3Util getInstance() {
    log.info("Creating S3Util bean for file serving");
    log.info(">>> roleArn {}", roleArn);
    try {
      if (roleArn != null && !roleArn.trim().isEmpty()) {
        log.info("Using STS role-based S3Util with role ARN: {}", roleArn);
        StsClient clientToUse = stsClient != null ? stsClient : StsClient.create();
        return new S3Util(clientToUse, roleArn);
      } else {
        log.info("Using default S3Util with default credentials");
        S3Client s3Client = S3Client.create();
        return new S3Util(s3Client);
      }
    } catch (Exception e) {
      log.error("Failed to create S3Utils", e);
      return null;
    }
  }
}
