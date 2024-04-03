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
