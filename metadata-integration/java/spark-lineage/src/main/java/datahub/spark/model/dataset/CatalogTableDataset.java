package datahub.spark.model.dataset;

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

  public CatalogTableDataset(CatalogTable table, FabricType fabricType) {
    this(table.qualifiedName(), fabricType);
  }

  public CatalogTableDataset(String dsName, FabricType fabricType) {
    this.urn = new DatasetUrn(new DataPlatformUrn("hive"), dsName, fabricType);
  }

  @Override
  public DatasetUrn urn() {
    return this.urn;
  }
}
