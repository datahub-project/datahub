package io.openlineage.spark.agent.vendor.bigquery.lifecycle.plan;

import io.openlineage.spark.agent.lifecycle.plan.QueryPlanVisitor;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import java.util.Collection;
import java.util.Collections;

public class BigQueryEventHandlerFactory implements OpenLineageEventHandlerFactory {
  @Override
  public Collection<QueryPlanVisitor<?, ?>> createEventHandlers(OpenLineageContext context) {
    // For now, we don't have specific handlers for BigQuery operations
    // This can be expanded in the future as needed
    return Collections.emptyList();
  }
}
