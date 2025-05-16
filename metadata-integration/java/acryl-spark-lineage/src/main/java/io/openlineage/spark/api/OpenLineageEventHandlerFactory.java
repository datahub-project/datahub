package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.QueryPlanVisitor;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import scala.PartialFunction;

/** Factory for creating OpenLineage event handlers. */
public interface OpenLineageEventHandlerFactory {
  /**
   * Get the list of QueryPlanVisitors provided by this factory for building OpenLineage output
   * datasets.
   *
   * @param context The OpenLineage context
   * @return A collection of QueryPlanVisitors
   */
  default Collection<QueryPlanVisitor<?, ?>> createEventHandlers(OpenLineageContext context) {
    return Collections.emptyList();
  }

  /**
   * Get the list of output dataset builders.
   *
   * @param context The OpenLineage context
   * @return A collection of PartialFunction objects for building output datasets
   */
  default Collection<PartialFunction<Object, List<OpenLineage.OutputDataset>>>
      createOutputDatasetBuilder(OpenLineageContext context) {
    return Collections.emptyList();
  }

  /**
   * Get the list of input dataset builders.
   *
   * @param context The OpenLineage context
   * @return A collection of PartialFunction objects for building input datasets
   */
  default Collection<PartialFunction<Object, List<OpenLineage.InputDataset>>>
      createInputDatasetBuilder(OpenLineageContext context) {
    return Collections.emptyList();
  }
}
