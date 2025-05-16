package io.openlineage.spark.agent.vendor.iceberg.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.AbstractQueryPlanDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.StructType;

/**
 * This is a placeholder implementation for handling Iceberg write operations. The actual
 * implementation would depend on the specific Iceberg class structure and operations.
 */
@Slf4j
public class IcebergWriteBuilder
    extends AbstractQueryPlanDatasetBuilder<
        SparkListenerEvent, LogicalPlan, OpenLineage.OutputDataset> {

  public IcebergWriteBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    // Check if this is an Iceberg write operation
    return plan.getClass().getName().contains("org.apache.iceberg")
        && (plan.getClass().getSimpleName().contains("WriteFiles")
            || plan.getClass().getSimpleName().contains("AppendData")
            || plan.getClass().getSimpleName().contains("OverwriteFiles"));
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan command) {
    try {
      log.info("Processing Iceberg write command: {}", command.getClass().getName());

      // Extract table information using reflection
      Method getTableMethod = findMethodByNamePattern(command.getClass(), "table|destination");
      if (getTableMethod != null) {
        Object tableObj = getTableMethod.invoke(command);
        if (tableObj != null) {
          // For Iceberg tables, we need to extract the name/identifier and namespace
          String tableName = extractTableName(tableObj);
          String namespace = extractNamespace(tableObj);

          // Get schema if available
          StructType schema = extractSchema(tableObj);

          // Construct dataset name
          String datasetName = namespace + "." + tableName;

          log.info("Found Iceberg write operation targeting table: {}", datasetName);

          return Collections.singletonList(buildDataset(datasetName, "iceberg", schema));
        }
      }

      log.debug("Could not extract table information from Iceberg write command");
      return Collections.emptyList();

    } catch (Exception e) {
      log.error("Failed to extract table information from Iceberg write command", e);
      return Collections.emptyList();
    }
  }

  private String extractTableName(Object tableObj) {
    try {
      Method getNameMethod = findMethodByNamePattern(tableObj.getClass(), "name");
      if (getNameMethod != null) {
        Object result = getNameMethod.invoke(tableObj);
        if (result != null) {
          return result.toString();
        }
      }

      // Fallback strategy - look for identifier methods
      Method getIdentifierMethod = findMethodByNamePattern(tableObj.getClass(), "ident|identifier");
      if (getIdentifierMethod != null) {
        Object result = getIdentifierMethod.invoke(tableObj);
        if (result != null) {
          // This might be a complex object, try to get just the name part
          try {
            Method nameMethod = findMethodByNamePattern(result.getClass(), "name");
            if (nameMethod != null) {
              Object nameResult = nameMethod.invoke(result);
              if (nameResult != null) {
                return nameResult.toString();
              }
            }
          } catch (Exception e) {
            // Just use the toString of the original object
            return result.toString();
          }
        }
      }
    } catch (Exception e) {
      log.debug("Exception extracting table name", e);
    }

    return "unknown_table";
  }

  private String extractNamespace(Object tableObj) {
    try {
      Method getNamespaceMethod =
          findMethodByNamePattern(tableObj.getClass(), "namespace|database");
      if (getNamespaceMethod != null) {
        Object result = getNamespaceMethod.invoke(tableObj);
        if (result != null) {
          return result.toString();
        }
      }
    } catch (Exception e) {
      log.debug("Exception extracting namespace", e);
    }

    return "default";
  }

  private StructType extractSchema(Object tableObj) {
    try {
      Method getSchemaMethod = findMethodByNamePattern(tableObj.getClass(), "schema");
      if (getSchemaMethod != null) {
        return (StructType) getSchemaMethod.invoke(tableObj);
      }
    } catch (Exception e) {
      log.debug("Exception extracting schema", e);
    }

    return null;
  }

  private Method findMethodByNamePattern(Class<?> clazz, String pattern) {
    for (Method method : clazz.getMethods()) {
      if (method.getName().matches(".*(" + pattern + ").*") && method.getParameterCount() == 0) {
        return method;
      }
    }
    return null;
  }
}
