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

  /** Path prefix for asset uploads in S3 */
  public String assetPathPrefix;

  /** Comma-separated list of allowed file extensions for upload */
  public String allowedFileExtensions;
}
