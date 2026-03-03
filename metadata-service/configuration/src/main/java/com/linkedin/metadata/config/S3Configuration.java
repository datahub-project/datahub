package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing the "datahub.s3" configuration block in application.yaml. */
@Data
public class S3Configuration {
  /** S3 bucket name */
  public String bucketName;

  /** S3 role ARN */
  public String roleArn;

  /** Expiration in seconds for presigned upload URLs */
  public Integer presignedUploadUrlExpirationSeconds;

  /** Expiration in seconds for presigned download URLs */
  public Integer presignedDownloadUrlExpirationSeconds;

  /** Path prefix for asset uploads in S3 */
  public String assetPathPrefix;
}
