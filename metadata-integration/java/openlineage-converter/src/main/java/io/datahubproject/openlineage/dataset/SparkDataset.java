/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.openlineage.dataset;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DatasetUrn;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.utils.DatahubUtils;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public abstract class SparkDataset {

  private final DatasetUrn urn;

  public SparkDataset(
      String platform, String platformInstance, String name, FabricType fabricType) {
    super();
    this.urn = DatahubUtils.createDatasetUrn(platform, platformInstance, name, fabricType);
  }

  public SparkDataset(String platform, String name, DatahubOpenlineageConfig datahubConfig) {
    super();
    this.urn =
        DatahubUtils.createDatasetUrn(
            platform,
            datahubConfig.getCommonDatasetPlatformInstance(),
            name,
            datahubConfig.getFabricType());
  }

  public DatasetUrn urn() {
    return urn;
  }
}
