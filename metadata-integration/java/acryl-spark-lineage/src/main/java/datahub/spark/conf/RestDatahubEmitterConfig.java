/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package datahub.spark.conf;

import datahub.client.rest.RestEmitterConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@ToString
@Getter
public class RestDatahubEmitterConfig implements DatahubEmitterConfig {
  final String type = "rest";
  datahub.client.rest.RestEmitterConfig restEmitterConfig;

  public RestDatahubEmitterConfig(RestEmitterConfig restEmitterConfig) {
    this.restEmitterConfig = restEmitterConfig;
  }
}
