package io.openlineage.spark.agent.vendor.iceberg.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.QueryPlanVisitor;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.StructType;

@Slf4j
public class IcebergCommandVisitor<D extends OpenLineage.Dataset>
    extends QueryPlanVisitor<LogicalPlan, D> {
  private static final String ICEBERG_NAMESPACE = "iceberg";
  private final DatasetFactory<D> factory;

  public IcebergCommandVisitor(@NonNull OpenLineageContext context, DatasetFactory<D> factory) {
    super(context);
    this.factory = factory;
    log.info("IcebergCommandVisitor created");
  }

  @Override
  public List<D> apply(LogicalPlan x) {
    try {
      // Use reflection to access Iceberg command properties
      String className = x.getClass().getName();
      log.info("Processing Iceberg command of type: {}", className);

      // Check if it's a known Iceberg command
      if (className.contains("org.apache.iceberg.spark.actions")
          || className.contains("org.apache.iceberg.spark.procedures")) {

        // Extract table information using reflection
        // For Iceberg, tables are often represented by the SparkTable class
        // Look for methods that might return table information
        Method getTableMethod = findMethodByNamePart(x.getClass(), "table");
        if (getTableMethod != null) {
          Object tableObj = getTableMethod.invoke(x);
          if (tableObj != null) {
            // Extract table name and namespace
            Method getNameMethod = findMethodByNamePart(tableObj.getClass(), "name");
            if (getNameMethod != null) {
              String tableName = (String) getNameMethod.invoke(tableObj);

              // Get schema if available
              StructType schema = null;
              try {
                Method getSchemaMethod = findMethodByNamePart(tableObj.getClass(), "schema");
                if (getSchemaMethod != null) {
                  schema = (StructType) getSchemaMethod.invoke(tableObj);
                }
              } catch (Exception e) {
                log.debug("Could not get schema from Iceberg table", e);
              }

              // Try to get namespace/database name
              String namespace = "default";
              try {
                Method getNamespaceMethod = findMethodByNamePart(tableObj.getClass(), "namespace");
                if (getNamespaceMethod != null) {
                  Object namespaceObj = getNamespaceMethod.invoke(tableObj);
                  if (namespaceObj != null) {
                    namespace = namespaceObj.toString();
                  }
                }
              } catch (Exception e) {
                log.debug("Could not get namespace from Iceberg table", e);
              }

              // Use the complete table name with namespace
              String fullTableName = namespace + "." + tableName;
              log.info("Found Iceberg operation targeting table: {}", fullTableName);

              return Collections.singletonList(
                  factory.getDataset(fullTableName, ICEBERG_NAMESPACE, schema));
            }
          }
        }
      }

      // If we couldn't extract the table information, log and return empty list
      log.debug("Could not extract table information from Iceberg command: {}", className);
      return Collections.emptyList();

    } catch (Exception e) {
      log.error("Failed to extract table information from Iceberg command", e);
      return Collections.emptyList();
    }
  }

  private Method findMethodByNamePart(Class<?> clazz, String namePart) {
    for (Method method : clazz.getMethods()) {
      if (method.getName().contains(namePart) && method.getParameterCount() == 0) {
        return method;
      }
    }
    return null;
  }

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    String className = plan.getClass().getName();
    // Check if this is an Iceberg-specific logical plan
    return className.contains("org.apache.iceberg");
  }
}
