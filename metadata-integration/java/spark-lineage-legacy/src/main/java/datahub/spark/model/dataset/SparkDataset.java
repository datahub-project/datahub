/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package datahub.spark.model.dataset;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DatasetUrn;
import datahub.spark.model.LineageUtils;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public abstract class SparkDataset {

  private DatasetUrn urn;

  public SparkDataset(
      String platform, String platformInstance, String name, FabricType fabricType) {
    super();
    this.urn = LineageUtils.createDatasetUrn(platform, platformInstance, name, fabricType);
  }

  public DatasetUrn urn() {
    return urn;
  }
}
