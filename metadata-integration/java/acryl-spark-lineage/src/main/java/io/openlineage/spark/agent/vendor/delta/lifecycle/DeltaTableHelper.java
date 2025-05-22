package io.openlineage.spark.agent.vendor.delta.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.types.StructType;

/** Helper class for extracting Delta Lake table information. */
@Slf4j
public class DeltaTableHelper {
  public static final String DELTA_NAMESPACE = "delta";

  private DeltaTableHelper() {}

  /**
   * Extracts a dataset from a Delta table name.
   *
   * @param factory Dataset factory for creating OpenLineage datasets
   * @param tableName The name of the Delta table
   * @param schema Optional schema for the table (can be null)
   * @return A list containing the output dataset
   */
  public static <D extends OpenLineage.Dataset> List<D> getDataset(
      DatasetFactory<D> factory, String tableName, StructType schema) {

    if (tableName == null || tableName.isEmpty()) {
      log.warn("Unable to create Delta dataset: tableName is null or empty");
      return Collections.emptyList();
    }

    if (schema != null) {
      return Collections.singletonList(factory.getDataset(tableName, DELTA_NAMESPACE, schema));
    } else {
      return Collections.singletonList(factory.getDataset(tableName, DELTA_NAMESPACE));
    }
  }
}
