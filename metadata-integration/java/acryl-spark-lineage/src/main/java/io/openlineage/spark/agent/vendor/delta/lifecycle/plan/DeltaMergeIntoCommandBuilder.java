package io.openlineage.spark.agent.vendor.delta.lifecycle.plan;

import static io.openlineage.spark.agent.vendor.delta.Constants.MERGE_INTO_COMMAND_CLASS;
import static io.openlineage.spark.agent.vendor.delta.DeltaVendor.hasDeltaClasses;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.vendor.delta.lifecycle.DeltaTableHelper;
import io.openlineage.spark.api.AbstractQueryPlanDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * This builder extracts the target table name from Delta Lake's MergeIntoCommand and adds it as an
 * output dataset with the proper naming.
 */
@Slf4j
public class DeltaMergeIntoCommandBuilder
    extends AbstractQueryPlanDatasetBuilder<
        SparkListenerEvent, LogicalPlan, OpenLineage.OutputDataset> {

  public DeltaMergeIntoCommandBuilder(OpenLineageContext context) {
    super(context, false);
  }

  public boolean isDefinedAt(LogicalPlan plan) {
    try {
      if (!hasDeltaClasses()) {
        return false;
      }
      Class<?> mergeIntoCommandClass = Class.forName(MERGE_INTO_COMMAND_CLASS);
      return mergeIntoCommandClass.isInstance(plan);
    } catch (ClassNotFoundException e) {
      log.debug("Delta Lake MergeIntoCommand class not found", e);
      return false;
    }
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan plan) {
    try {
      // Get the target table name using reflection
      String tableName = extractTargetTableName(plan);
      if (tableName == null) {
        log.warn("Could not extract table name from Delta MergeIntoCommand");
        return Collections.emptyList();
      }

      log.info("Detected Delta Lake MERGE INTO command for table: {}", tableName);

      // Create a proper output dataset with the table name using our helper
      return DeltaTableHelper.getDataset(outputDataset(), tableName, null);
    } catch (Exception e) {
      log.error("Error processing Delta Lake MergeIntoCommand", e);
      return Collections.emptyList();
    }
  }

  /** Extracts the target table name from a MergeIntoCommand using reflection. */
  private String extractTargetTableName(LogicalPlan plan) {
    try {
      // Try to get the target field from MergeIntoCommand
      Method getTarget = plan.getClass().getMethod("target");
      Object target = getTarget.invoke(plan);

      if (target == null) {
        return null;
      }

      // Target could be a LogicalRelation or another relation type
      // We need to recursively check for common table name fields
      return extractTableNameFromRelation(target);
    } catch (Exception e) {
      log.debug("Could not extract target table name from MergeIntoCommand", e);
      return null;
    }
  }

  /**
   * Recursively tries to extract table name from a relation object. This handles different versions
   * of Delta Lake and different relation types.
   */
  private String extractTableNameFromRelation(Object relation) {
    if (relation == null) {
      return null;
    }

    try {
      // Try common field names for table identification
      for (String methodName :
          new String[] {"tableName", "tableIdentifier", "identifier", "name", "tablePath"}) {
        try {
          Method method = relation.getClass().getMethod(methodName);
          Object result = method.invoke(relation);
          if (result != null) {
            return result.toString();
          }
        } catch (NoSuchMethodException e) {
          // Skip if method doesn't exist
        }
      }

      // If relation has a 'relation' field, recursively check it
      try {
        Method getRelation = relation.getClass().getMethod("relation");
        Object nestedRelation = getRelation.invoke(relation);
        return extractTableNameFromRelation(nestedRelation);
      } catch (NoSuchMethodException e) {
        // Skip if method doesn't exist
      }

      // If relation has a 'table' field, recursively check it
      try {
        Method getTable = relation.getClass().getMethod("table");
        Object table = getTable.invoke(relation);
        return extractTableNameFromRelation(table);
      } catch (NoSuchMethodException e) {
        // Skip if method doesn't exist
      }

      // Try to get table from 'catalogTable' if it exists
      try {
        Method getCatalogTable = relation.getClass().getMethod("catalogTable");
        Object catalogTable = getCatalogTable.invoke(relation);
        if (catalogTable != null) {
          if (catalogTable instanceof CatalogTable) {
            CatalogTable table = (CatalogTable) catalogTable;
            if (table.identifier() != null) {
              return table.identifier().toString();
            }
          } else {
            // Fallback to reflection if not the expected type
            try {
              Method getIdentifier = catalogTable.getClass().getMethod("identifier");
              Object identifier = getIdentifier.invoke(catalogTable);
              if (identifier != null) {
                return identifier.toString();
              }
            } catch (NoSuchMethodException e) {
              // Skip if method doesn't exist
            }
          }
        }
      } catch (NoSuchMethodException e) {
        // Skip if method doesn't exist
      }

      // As a last resort, try to get the toString() of the relation
      // and extract a meaningful name from it
      String relString = relation.toString();
      if (relString.contains("Table") && relString.contains("[")) {
        int start = relString.indexOf("[") + 1;
        int end = relString.indexOf("]", start);
        if (start > 0 && end > start) {
          return relString.substring(start, end);
        }
      }

      return null;
    } catch (Exception e) {
      log.debug("Error extracting table name from relation", e);
      return null;
    }
  }
}
