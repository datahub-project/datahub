/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package datahub.spark.conf;

import datahub.client.s3.S3EmitterConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@ToString
@Getter
public class S3DatahubEmitterConfig implements DatahubEmitterConfig {
  final String type = "s3";
  S3EmitterConfig s3EmitterConfig;

  public S3DatahubEmitterConfig(S3EmitterConfig s3EmitterConfig) {
    this.s3EmitterConfig = s3EmitterConfig;
  }
}
