package datahub.spark.model.dataset;

import org.apache.spark.sql.catalyst.catalog.CatalogTable;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DatasetUrn;

import datahub.spark.model.LineageUtils;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class CatalogTableDataset implements SparkDataset {
  private final DatasetUrn urn;

  public CatalogTableDataset(CatalogTable table, String platformInstance, FabricType fabricType) {
    this(table.qualifiedName(), platformInstance, fabricType);
  }

  public CatalogTableDataset(String dsName, String platformInstance, FabricType fabricType) {
    this.urn = LineageUtils.createDatasetUrn("hive", platformInstance, dsName, fabricType);
  }

  @Override
  public DatasetUrn urn() {
    return this.urn;
  }
}
