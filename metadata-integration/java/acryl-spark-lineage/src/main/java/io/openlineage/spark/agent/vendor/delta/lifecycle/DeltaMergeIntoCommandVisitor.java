package io.openlineage.spark.agent.vendor.delta.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.agent.vendor.delta.DeltaVendor;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.StructType;

/**
 * {@link LogicalPlan} visitor that matches {@link DeltaMergeIntoCommand}s. This function extracts a
 * {@link OpenLineage.Dataset} from the Delta table referenced by the command.
 */
@Slf4j
public class DeltaMergeIntoCommandVisitor<D extends OpenLineage.Dataset>
    extends QueryPlanVisitor<LogicalPlan, D> {
  private static final String DELTA_NAMESPACE = "delta";
  private final DatasetFactory<D> factory;

  public DeltaMergeIntoCommandVisitor(
      @NonNull OpenLineageContext context, DatasetFactory<D> factory) {
    super(context);
    this.factory = factory;
    log.info("DeltaMergeIntoCommandVisitor created");
  }

  @Override
  public List<D> apply(LogicalPlan x) {
    try {
      // Use reflection to access DeltaMergeIntoCommand properties
      // Get the target tableIdentifier
      Method getTargetTableMethod = x.getClass().getMethod("target");
      Object targetTable = getTargetTableMethod.invoke(x);

      Method getTableIdentifierMethod = targetTable.getClass().getMethod("tableIdentifier");
      TableIdentifier tableIdentifier =
          (TableIdentifier) getTableIdentifierMethod.invoke(targetTable);

      // Get schema if available
      StructType schema = null;
      try {
        Method getSchemaMethod = targetTable.getClass().getMethod("schema");
        schema = (StructType) getSchemaMethod.invoke(targetTable);
      } catch (Exception e) {
        log.debug("Could not get schema from target table", e);
      }

      String tableName = tableIdentifier.table();
      String databaseName =
          ScalaConversionUtils.asJavaOptional(tableIdentifier.database()).orElse("default");

      // Use the complete table name with database
      String fullTableName = databaseName + "." + tableName;
      log.info("Found Delta merge command targeting table: {}", fullTableName);

      return Collections.singletonList(factory.getDataset(fullTableName, DELTA_NAMESPACE, schema));

    } catch (Exception e) {
      log.error("Failed to extract table information from Delta merge command", e);
      return Collections.emptyList();
    }
  }

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    try {
      Class<?> deltaCommandClass =
          Thread.currentThread()
              .getContextClassLoader()
              .loadClass(DeltaVendor.DELTA_MERGE_INTO_COMMAND);
      return deltaCommandClass.isAssignableFrom(plan.getClass());
    } catch (Exception e) {
      return false;
    }
  }
}
