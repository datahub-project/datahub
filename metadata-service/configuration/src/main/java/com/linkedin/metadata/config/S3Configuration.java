/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
