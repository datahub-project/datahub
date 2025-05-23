package io.openlineage.spark.agent.vendor.hudi.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.AbstractQueryPlanDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.StructType;

/**
 * This is a placeholder implementation for handling Hudi merge operations. The actual
 * implementation would depend on the specific Hudi class structure and operations.
 */
@Slf4j
public class HudiMergeIntoCommandBuilder
    extends AbstractQueryPlanDatasetBuilder<
        SparkListenerEvent, LogicalPlan, OpenLineage.OutputDataset> {

  public HudiMergeIntoCommandBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    // Check if this is a Hudi merge operation
    return plan.getClass().getName().contains("org.apache.hudi")
        && plan.getClass().getSimpleName().contains("MergeInto");
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan command) {
    try {
      log.info("Processing Hudi merge command: {}", command.getClass().getName());

      // Extract table information using reflection
      Method getTargetTableMethod = findMethodContaining(command.getClass(), "targetTable");
      if (getTargetTableMethod != null) {
        Object tableObj = getTargetTableMethod.invoke(command);
        if (tableObj != null) {
          // Extract table identifier
          Method getTableIdentifierMethod = findMethodContaining(tableObj.getClass(), "identifier");
          if (getTableIdentifierMethod != null) {
            TableIdentifier tableIdentifier =
                (TableIdentifier) getTableIdentifierMethod.invoke(tableObj);

            // Get schema if available
            StructType schema = null;
            try {
              Method getSchemaMethod = findMethodContaining(tableObj.getClass(), "schema");
              if (getSchemaMethod != null) {
                schema = (StructType) getSchemaMethod.invoke(tableObj);
              }
            } catch (Exception e) {
              log.debug("Could not get schema from Hudi table", e);
            }

            String tableName = tableIdentifier.table();
            String databaseName =
                Optional.ofNullable(tableIdentifier.database())
                    .map(scala.Option::get)
                    .orElse("default");

            // Construct dataset name and namespace
            String datasetName = databaseName + "." + tableName;
            String namespace = "hudi";

            log.info("Found Hudi merge operation targeting table: {}", datasetName);

            return Collections.singletonList(buildDataset(datasetName, namespace, schema));
          }
        }
      }

      log.debug("Could not extract table information from Hudi merge command");
      return Collections.emptyList();

    } catch (Exception e) {
      log.error("Failed to extract table information from Hudi merge command", e);
      return Collections.emptyList();
    }
  }

  private Method findMethodContaining(Class<?> clazz, String nameContains) {
    for (Method method : clazz.getMethods()) {
      if (method.getName().contains(nameContains) && method.getParameterCount() == 0) {
        return method;
      }
    }
    return null;
  }
}
