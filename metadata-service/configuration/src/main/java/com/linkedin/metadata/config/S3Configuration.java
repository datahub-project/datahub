package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing the "datahub.s3" configuration block in application.yaml.on.yml */
@Data
public class S3Configuration {
  /** S3 bucket name */
  public String bucketName;

  /** S3 role ARN */
  public String roleArn;
}
