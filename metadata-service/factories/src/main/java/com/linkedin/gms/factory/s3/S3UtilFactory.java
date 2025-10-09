package com.linkedin.gms.factory.s3;

import com.linkedin.datahub.graphql.util.S3Util;
import com.linkedin.entity.client.EntityClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

import javax.annotation.Nonnull;

@Slf4j
@Configuration
public class S3UtilFactory {

  @Autowired
  @Qualifier("entityClient")
  private EntityClient entityClient;

  @Value("${datahub.files.s3.executorRoleArn:#{null}}")
  private String executorRoleArn;

  @Bean(name = "s3Util")
  @Nonnull
  protected S3Util getInstance() {
    log.info("Creating S3Util bean for file serving");
    
    if (executorRoleArn != null && !executorRoleArn.trim().isEmpty()) {
      log.info("Using STS role-based S3Util with role ARN: {}", executorRoleArn);
      StsClient stsClient = StsClient.create();
      return new S3Util(entityClient, stsClient, executorRoleArn);
    } else {
      log.info("Using default S3Util with default credentials");
      S3Client s3Client = S3Client.create();
      return new S3Util(s3Client, entityClient);
    }
  }
}
