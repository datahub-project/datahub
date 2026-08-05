package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing the "datahub.objectStorage" configuration block in application.yaml. */
@Data
public class ObjectStorageConfiguration {

  /**
   * Primary location URI: {@code s3://bucket}, {@code s3://bucket/prefix}, {@code gs://bucket},
   * {@code gs://bucket/prefix}, or {@code file:///path}. Bound from {@code
   * DATAHUB_OBJECT_STORAGE_URI}.
   */
  private String uri;

  /**
   * Legacy bucket name for URI synthesis when {@link #uri} is unset. Bound from {@code
   * DATAHUB_BUCKET_NAME}.
   */
  private String bucket;

  /** Legacy in-bucket prefix or local root when {@link #uri} is unset. */
  private String path;

  /**
   * Legacy provider hint when {@link #uri} is unset. Bound from {@code
   * DATAHUB_OBJECT_STORAGE_PROVIDER}.
   */
  private String provider;

  /** AWS IAM role ARN to assume for S3 access. Bound from {@code DATAHUB_ROLE_ARN}. */
  private String roleArn;

  private int presignedUploadUrlExpirationSeconds;

  private int presignedDownloadUrlExpirationSeconds;

  /** Path prefix for documentation file uploads (e.g. {@code product_assets}). */
  private String assetPathPrefix;

  private Integer multipartThresholdBytes;

  private Integer multipartPartSizeBytes;
}
