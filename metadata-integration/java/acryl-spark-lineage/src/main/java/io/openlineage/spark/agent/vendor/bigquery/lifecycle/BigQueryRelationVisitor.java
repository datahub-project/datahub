package io.openlineage.spark.agent.vendor.bigquery.lifecycle;

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
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.types.StructType;

@Slf4j
public class BigQueryRelationVisitor<D extends OpenLineage.Dataset>
    extends QueryPlanVisitor<LogicalPlan, D> {
  private static final String BIGQUERY_NAMESPACE = "bigquery";
  private final DatasetFactory<D> factory;

  public BigQueryRelationVisitor(@NonNull OpenLineageContext context, DatasetFactory<D> factory) {
    super(context);
    this.factory = factory;
    log.info("BigQueryRelationVisitor created");
  }

  @Override
  public List<D> apply(LogicalPlan x) {
    try {
      // BigQuery integration typically uses LogicalRelation with a BigQueryRelation
      if (x instanceof LogicalRelation) {
        LogicalRelation relation = (LogicalRelation) x;
        Object bigQueryRelation = relation.relation();

        if (bigQueryRelation.getClass().getName().contains("bigquery")) {
          log.info("Processing BigQuery relation: {}", bigQueryRelation.getClass().getName());

          // Extract table information using reflection
          String projectId = extractProperty(bigQueryRelation, "project");
          String datasetId = extractProperty(bigQueryRelation, "dataset");
          String tableId = extractProperty(bigQueryRelation, "table");

          if (tableId != null) {
            // Get schema from the relation's output
            StructType schema = extractSchema(relation);

            // Construct the full table name in BigQuery format: project.dataset.table
            String fullTableName = constructTableName(projectId, datasetId, tableId);
            log.info("Found BigQuery table: {}", fullTableName);

            return Collections.singletonList(
                factory.getDataset(fullTableName, BIGQUERY_NAMESPACE, schema));
          }
        }
      }

      return Collections.emptyList();
    } catch (Exception e) {
      log.error("Failed to extract table information from BigQuery relation", e);
      return Collections.emptyList();
    }
  }

  private String extractProperty(Object bigQueryRelation, String propertyName) {
    try {
      // Try getter method first (e.g., getProject(), getTable())
      String getterName =
          "get" + propertyName.substring(0, 1).toUpperCase() + propertyName.substring(1);
      Method getterMethod = findMethod(bigQueryRelation.getClass(), getterName);

      if (getterMethod != null) {
        Object result = getterMethod.invoke(bigQueryRelation);
        if (result != null) {
          return result.toString();
        }
      }

      // Try field access method (e.g., project(), table())
      Method fieldMethod = findMethod(bigQueryRelation.getClass(), propertyName);
      if (fieldMethod != null) {
        Object result = fieldMethod.invoke(bigQueryRelation);
        if (result != null) {
          return result.toString();
        }
      }
    } catch (Exception e) {
      log.debug("Could not extract '{}' property from BigQuery relation", propertyName, e);
    }

    return null;
  }

  private Method findMethod(Class<?> clazz, String name) {
    try {
      Method method = clazz.getMethod(name);
      return method;
    } catch (NoSuchMethodException e) {
      return null;
    }
  }

  private StructType extractSchema(LogicalRelation relation) {
    try {
      // Just return the schema directly without using JavaConverters
      return relation.schema();
    } catch (Exception e) {
      log.debug("Could not extract schema from BigQuery relation", e);
    }

    return null;
  }

  private String constructTableName(String projectId, String datasetId, String tableId) {
    StringBuilder builder = new StringBuilder();

    if (projectId != null) {
      builder.append(projectId).append(".");
    }

    if (datasetId != null) {
      builder.append(datasetId).append(".");
    }

    builder.append(tableId);

    return builder.toString();
  }

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    if (plan instanceof LogicalRelation) {
      LogicalRelation relation = (LogicalRelation) plan;
      String relationClassName = relation.relation().getClass().getName();
      return relationClassName.contains("bigquery");
    }
    return false;
  }
}
