package io.openlineage.spark.agent.vendor.hudi.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.QueryPlanVisitor;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.StructType;
import scala.Option;

@Slf4j
public class HudiCommandVisitor<D extends OpenLineage.Dataset>
    extends QueryPlanVisitor<LogicalPlan, D> {
  private static final String HUDI_NAMESPACE = "hudi";
  private final DatasetFactory<D> factory;

  public HudiCommandVisitor(@NonNull OpenLineageContext context, DatasetFactory<D> factory) {
    super(context);
    this.factory = factory;
    log.info("HudiCommandVisitor created");
  }

  @Override
  public List<D> apply(LogicalPlan x) {
    try {
      // Use reflection to access Hudi command properties
      // This is a simplified example, you'll need to adapt this to the specific Hudi operations

      // For example, to get table information for a HoodieMergeIntoCommand
      String className = x.getClass().getName();
      log.info("Processing Hudi command of type: {}", className);

      // Check if it's a known Hudi command
      if (className.contains("org.apache.hudi.execution")) {
        // Extract table information using reflection
        Method getTableMethod = findMethodContaining(x.getClass(), "table");
        if (getTableMethod != null) {
          Object tableObj = getTableMethod.invoke(x);
          if (tableObj != null) {
            // Extract table identifier
            Method getTableIdentifierMethod =
                findMethodContaining(tableObj.getClass(), "identifier");
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
                      .map(Option::get)
                      .orElse("default");

              // Use the complete table name with database
              String fullTableName = databaseName + "." + tableName;
              log.info("Found Hudi operation targeting table: {}", fullTableName);

              return Collections.singletonList(
                  factory.getDataset(fullTableName, HUDI_NAMESPACE, schema));
            }
          }
        }
      }

      // If we couldn't extract the table information, log and return empty list
      log.debug("Could not extract table information from Hudi command: {}", className);
      return Collections.emptyList();

    } catch (Exception e) {
      log.error("Failed to extract table information from Hudi command", e);
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

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    String className = plan.getClass().getName();
    // Check if this is a Hudi-specific logical plan
    return className.startsWith("org.apache.hudi");
  }
}
