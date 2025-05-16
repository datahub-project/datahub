package io.openlineage.spark.agent.vendor.delta.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.QueryPlanVisitor;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import scala.PartialFunction;

public class DeltaEventHandlerFactory implements OpenLineageEventHandlerFactory {
  @Override
  public Collection<PartialFunction<Object, List<OpenLineage.OutputDataset>>>
      createOutputDatasetBuilder(OpenLineageContext context) {
    // The right function will be determined at runtime by using type checking based on the correct
    // Spark LogicalPlan
    return Collections.singleton((PartialFunction) new DeltaMergeIntoCommandBuilder(context));
  }

  @Override
  public Collection<QueryPlanVisitor<?, ?>> createEventHandlers(OpenLineageContext context) {
    // For now, we don't have any Delta-specific plan handlers
    return Collections.emptyList();
  }
}
