package io.datahubproject.openlineage.dataset;

import com.linkedin.common.FabricType;
import lombok.ToString;

@ToString
public class CatalogTableDataset extends SparkDataset {

  public CatalogTableDataset(
      String dsName, String platformInstance, String platform, FabricType fabricType) {
    super(platform, platformInstance, dsName, fabricType);
  }
}
