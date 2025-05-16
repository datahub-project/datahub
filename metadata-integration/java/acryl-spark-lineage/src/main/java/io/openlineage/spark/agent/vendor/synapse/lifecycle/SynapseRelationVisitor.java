package io.openlineage.spark.agent.vendor.synapse.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.QueryPlanVisitor;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.types.StructType;

@Slf4j
public class SynapseRelationVisitor<D extends OpenLineage.Dataset>
    extends QueryPlanVisitor<LogicalPlan, D> {
  private static final String SYNAPSE_NAMESPACE = "synapse";
  private final DatasetFactory<D> factory;

  public SynapseRelationVisitor(@NonNull OpenLineageContext context, DatasetFactory<D> factory) {
    super(context);
    this.factory = factory;
    log.info("SynapseRelationVisitor created");
  }

  @Override
  public List<D> apply(LogicalPlan x) {
    try {
      // Synapse integration typically uses LogicalRelation with a SynapseRelation
      if (x instanceof LogicalRelation) {
        LogicalRelation relation = (LogicalRelation) x;
        Object synapseRelation = relation.relation();

        if (isSynapseRelation(synapseRelation)) {
          log.info("Processing Synapse relation: {}", synapseRelation.getClass().getName());

          // Extract table information using reflection
          Map<String, String> parameters = extractParameters(synapseRelation);

          if (parameters != null) {
            String serverName = parameters.get("serverName");
            String databaseName = parameters.get("databaseName");
            String tableName = parameters.get("tableName");

            if (tableName != null) {
              // Get schema from the relation's output
              StructType schema = extractSchema(relation);

              // Construct the full table name in Synapse format: server.database.table
              String fullTableName = constructTableName(serverName, databaseName, tableName);
              log.info("Found Synapse table: {}", fullTableName);

              return Collections.singletonList(
                  factory.getDataset(fullTableName, SYNAPSE_NAMESPACE, schema));
            }
          }
        }
      }

      return Collections.emptyList();
    } catch (Exception e) {
      log.error("Failed to extract table information from Synapse relation", e);
      return Collections.emptyList();
    }
  }

  private boolean isSynapseRelation(Object relation) {
    return relation.getClass().getName().contains("synapse");
  }

  @SuppressWarnings("unchecked")
  private Map<String, String> extractParameters(Object synapseRelation) {
    try {
      // Try to find parameters through reflection
      String[] methodNames = {"parameters", "getParameters", "options", "getOptions"};

      for (String methodName : methodNames) {
        try {
          Method method = synapseRelation.getClass().getMethod(methodName);
          if (method != null) {
            Object result = method.invoke(synapseRelation);
            if (result instanceof Map) {
              return (Map<String, String>) result;
            }
          }
        } catch (NoSuchMethodException e) {
          // Continue to the next method
        }
      }
    } catch (Exception e) {
      log.debug("Could not extract parameters from Synapse relation", e);
    }

    return null;
  }

  private StructType extractSchema(LogicalRelation relation) {
    try {
      // Just return the schema directly without using JavaConverters
      return relation.schema();
    } catch (Exception e) {
      log.debug("Could not extract schema from Synapse relation", e);
    }

    return null;
  }

  private String constructTableName(String serverName, String databaseName, String tableName) {
    StringBuilder builder = new StringBuilder();

    if (serverName != null) {
      builder.append(serverName).append(".");
    }

    if (databaseName != null) {
      builder.append(databaseName).append(".");
    }

    builder.append(tableName);

    return builder.toString();
  }

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    if (plan instanceof LogicalRelation) {
      LogicalRelation relation = (LogicalRelation) plan;
      String relationClassName = relation.relation().getClass().getName();
      return relationClassName.contains("synapse");
    }
    return false;
  }
}
