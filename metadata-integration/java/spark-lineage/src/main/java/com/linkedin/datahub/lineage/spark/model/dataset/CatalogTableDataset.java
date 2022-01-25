package com.linkedin.datahub.lineage.spark.model.dataset;

import org.apache.spark.sql.catalyst.catalog.CatalogTable;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;

import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class CatalogTableDataset implements SparkDataset {
  private final DatasetUrn urn;

  public CatalogTableDataset(CatalogTable table) {
    this(table.qualifiedName());
  }

  public CatalogTableDataset(String dsName) {
    this.urn = new DatasetUrn(new DataPlatformUrn("hive"), dsName, FabricType.PROD);
  }

  @Override
  public DatasetUrn urn() {
    return this.urn;
  }
}
