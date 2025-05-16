package io.openlineage.spark.agent.vendor.hudi.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import scala.PartialFunction;

public class HudiEventHandlerFactory implements OpenLineageEventHandlerFactory {
  @Override
  public Collection<PartialFunction<Object, List<OpenLineage.OutputDataset>>>
      createOutputDatasetBuilder(OpenLineageContext context) {
    // Add specific handlers for Hudi operations as output dataset builders
    return Collections.singleton((PartialFunction) new HudiMergeIntoCommandBuilder(context));
  }
}
