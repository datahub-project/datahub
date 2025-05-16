package io.openlineage.spark.agent.vendor.delta.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.agent.vendor.delta.DeltaVendor;
import io.openlineage.spark.api.AbstractQueryPlanDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.StructType;

/** Extracts output datasets from Delta Lake merge commands. */
@Slf4j
public class DeltaMergeIntoCommandBuilder
    extends AbstractQueryPlanDatasetBuilder<
        SparkListenerEvent, LogicalPlan, OpenLineage.OutputDataset> {

  public DeltaMergeIntoCommandBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan command) {
    try {
      // Use reflection to access DeltaMergeIntoCommand properties
      // Get the target tableIdentifier
      Method getTargetTableMethod = command.getClass().getMethod("target");
      Object targetTable = getTargetTableMethod.invoke(command);

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
      log.info(
          "DeltaMergeIntoCommandBuilder found Delta merge command targeting table: {}",
          fullTableName);

      return Collections.singletonList(outputDataset().getDataset(fullTableName, "delta", schema));
    } catch (Exception e) {
      log.error("Failed to extract table information from Delta merge command", e);
      return Collections.emptyList();
    }
  }

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
