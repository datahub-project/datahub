package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collection;
import lombok.NonNull;
import scala.PartialFunction;

/**
 * Base class for LogicalPlan visitors. This class serves as the base for implementing visitors for
 * specific LogicalPlan types.
 *
 * @param <P> The type of plan to visit
 * @param <D> The type of dataset to return
 */
public abstract class QueryPlanVisitor<P, D> implements PartialFunction<P, Collection<D>> {

  protected final OpenLineageContext context;

  /**
   * Constructs a new QueryPlanVisitor
   *
   * @param context the OpenLineageContext for the visitor
   */
  public QueryPlanVisitor(@NonNull OpenLineageContext context) {
    this.context = context;
  }
}
