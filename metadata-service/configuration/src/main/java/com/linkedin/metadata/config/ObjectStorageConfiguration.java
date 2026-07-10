package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing the "datahub.objectStorage" configuration block in application.yaml. */
@Data
public class ObjectStorageConfiguration {
  /** Optional in-bucket prefix or local filesystem root when bucket is unset. */
  private String path;

  /** Optional provider hint: s3, gcs, or local. */
  private String provider;

  private Integer multipartThresholdBytes;

  private Integer multipartPartSizeBytes;
}
