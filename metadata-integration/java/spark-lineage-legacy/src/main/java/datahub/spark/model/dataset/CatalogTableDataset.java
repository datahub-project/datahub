package datahub.spark.model.dataset;

import com.linkedin.common.FabricType;
import lombok.ToString;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;

@ToString
public class CatalogTableDataset extends SparkDataset {

  /** Spark's default catalog identifier, used in CatalogTable.qualifiedName() from Spark 3.5+ */
  private static final String SPARK_DEFAULT_CATALOG = "spark_catalog.";

  public CatalogTableDataset(
      CatalogTable table, String platformInstance, String platform, FabricType fabricType) {
    this(table.qualifiedName(), platformInstance, platform, fabricType);
  }

  public CatalogTableDataset(
      String dsName, String platformInstance, String platform, FabricType fabricType) {
    super(platform, platformInstance, stripDefaultCatalog(dsName), fabricType);
  }

  /**
   * Strip the default catalog prefix from Spark table names. Spark 3.5+ includes the catalog name
   * in CatalogTable.qualifiedName(); for the default catalog, we strip it to maintain backward
   * compatibility with lineage URNs created on older Spark versions.
   *
   * <p>NOTE: Only strips the Spark default catalog ("spark_catalog."). Preserves all other catalog
   * prefixes since they are meaningful in multi-catalog setups.
   */
  private static String stripDefaultCatalog(String name) {
    if (name != null && name.startsWith(SPARK_DEFAULT_CATALOG)) {
      return name.substring(SPARK_DEFAULT_CATALOG.length());
    }
    return name;
  }
}
