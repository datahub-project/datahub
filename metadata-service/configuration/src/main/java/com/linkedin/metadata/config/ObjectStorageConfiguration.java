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

  /** Legacy in-bucket prefix or local root when {@link #uri} is unset. */
  private String path;

  /**
   * Legacy provider hint when {@link #uri} is unset. Bound from {@code
   * DATAHUB_OBJECT_STORAGE_PROVIDER}.
   */
  private String provider;

  private Integer multipartThresholdBytes;

  private Integer multipartPartSizeBytes;
}
