package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.List;
import lombok.NonNull;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.StructType;
import scala.PartialFunction;

/**
 * Base abstract class for dataset builders that extract datasets from query plans.
 *
 * @param <E> The event type
 * @param <P> The plan type
 * @param <D> The dataset type
 */
public abstract class AbstractQueryPlanDatasetBuilder<E, P, D>
    implements PartialFunction<P, List<D>> {

  protected final OpenLineageContext context;
  protected final boolean searchDependencies;

  /**
   * Constructor for AbstractQueryPlanDatasetBuilder
   *
   * @param context the OpenLineage context
   * @param searchDependencies whether to search dependencies
   */
  public AbstractQueryPlanDatasetBuilder(
      @NonNull OpenLineageContext context, boolean searchDependencies) {
    this.context = context;
    this.searchDependencies = searchDependencies;
  }

  /**
   * Helper method to get a schema from a logical plan
   *
   * @param logicalPlan The logical plan to extract schema from
   * @return The extracted schema
   */
  protected StructType getSchema(LogicalPlan logicalPlan) {
    return logicalPlan.schema();
  }

  /**
   * Helper method to get an OpenLineage OutputDatasetBuilder
   *
   * @return an OutputDatasetBuilder
   */
  protected OpenLineage.OutputDatasetBuilder outputDataset() {
    return context.getOpenLineage().newOutputDatasetBuilder();
  }

  /**
   * Helper method to create a dataset from name, namespace and schema
   *
   * @param name The dataset name
   * @param namespace The dataset namespace
   * @param schema The dataset schema
   * @return An output dataset
   */
  protected OpenLineage.OutputDataset buildDataset(
      String name, String namespace, StructType schema) {
    return outputDataset().name(name).namespace(namespace).build();
  }

  @Override
  public boolean isDefinedAt(P x) {
    return true;
  }
}
