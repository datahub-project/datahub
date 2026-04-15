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
    super(platform, platformInstance, stripDefaultCatalog(dsName), fabricType);
  }

  /**
   * Strip the default catalog prefix (e.g. "spark_catalog.") from table names. Spark 3.5+ includes
   * the catalog name in CatalogTable.qualifiedName(), but this breaks URN compatibility with older
   * versions and is redundant for the default catalog.
   */
  private static String stripDefaultCatalog(String name) {
    if (name != null && name.startsWith("spark_catalog.")) {
      return name.substring("spark_catalog.".length());
    }
    return name;
  }
}
