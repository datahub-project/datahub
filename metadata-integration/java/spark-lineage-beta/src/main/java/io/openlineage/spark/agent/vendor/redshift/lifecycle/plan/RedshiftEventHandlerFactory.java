package io.openlineage.spark.agent.vendor.redshift.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import scala.PartialFunction;

public class RedshiftEventHandlerFactory implements OpenLineageEventHandlerFactory {
  @Override
  public Collection<PartialFunction<Object, List<OpenLineage.OutputDataset>>>
      createOutputDatasetBuilder(OpenLineageContext context) {
    // The right function will be determined at runtime by using type checking based on the correct
    // Spark LogicalPlan
    return Collections.singleton(
        (PartialFunction) new RedshiftSaveIntoDataSourceCommandBuilder(context));
  }
}
