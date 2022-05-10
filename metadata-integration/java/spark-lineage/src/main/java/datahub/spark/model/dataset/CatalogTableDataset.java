package datahub.spark.model.dataset;

import org.apache.spark.sql.catalyst.catalog.CatalogTable;

import com.linkedin.common.FabricType;

import lombok.ToString;

@ToString
public class CatalogTableDataset extends SparkDataset {

  public CatalogTableDataset(CatalogTable table, String platformInstance, FabricType fabricType) {
    this(table.qualifiedName(), platformInstance, fabricType);
  }

  public CatalogTableDataset(String dsName, String platformInstance, FabricType fabricType) {
    super("hive", platformInstance, dsName, fabricType);
  }

  public CatalogTableDataset(String dbName, String dsName, String platformInstance, FabricType fabricType) {
    super(dbName, platformInstance, dsName, fabricType);
  }
}
