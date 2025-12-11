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
import lombok.ToString;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;

@ToString
public class CatalogTableDataset extends SparkDataset {

  public CatalogTableDataset(
      CatalogTable table, String platformInstance, String platform, FabricType fabricType) {
    this(table.qualifiedName(), platformInstance, platform, fabricType);
  }

  public CatalogTableDataset(
      String dsName, String platformInstance, String platform, FabricType fabricType) {
    super(platform, platformInstance, dsName, fabricType);
  }
}
