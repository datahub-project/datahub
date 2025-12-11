/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package datahub.spark.conf;

import datahub.client.file.FileEmitterConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@ToString
@Getter
public class FileDatahubEmitterConfig implements DatahubEmitterConfig {
  final String type = "file";
  FileEmitterConfig fileEmitterConfig;

  public FileDatahubEmitterConfig(FileEmitterConfig fileEmitterConfig) {
    this.fileEmitterConfig = fileEmitterConfig;
  }
}
