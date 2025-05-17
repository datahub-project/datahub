package io.openlineage.spark.agent.vendor.iceberg.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import scala.PartialFunction;

public class IcebergEventHandlerFactory implements OpenLineageEventHandlerFactory {
  @Override
  public Collection<PartialFunction<Object, List<OpenLineage.OutputDataset>>>
      createOutputDatasetBuilder(OpenLineageContext context) {
    // Add specific handlers for Iceberg operations as output dataset builders
    return Collections.singleton((PartialFunction) new IcebergWriteBuilder(context));
  }
}
