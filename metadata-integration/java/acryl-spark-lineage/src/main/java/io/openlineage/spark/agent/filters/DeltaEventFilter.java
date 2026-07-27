/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import static io.openlineage.spark.agent.filters.EventFilterUtils.getLogicalPlan;
import static io.openlineage.spark.agent.filters.EventFilterUtils.isDeltaPlan;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.SerializeFromObject;
import org.apache.spark.sql.catalyst.trees.TreeNode;
import org.apache.spark.sql.execution.LogicalRDD;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlanInfo;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import scala.collection.JavaConverters;

/**
 * DataHub override of OpenLineage's Delta filter.
 *
 * <p>Delta MERGE operations run through job-level Spark listener events. OpenLineage normally
 * filters those events as duplicates, which suppresses lineage for {@code DeltaTable.merge()}.
 * Preserve the upstream noise filters for every other Delta operation, but allow MERGE events to
 * continue through the listener.
 */
@Slf4j
public class DeltaEventFilter implements EventFilter {

  private static final String MERGE_INTO_COMMAND_SUFFIX = ".MergeIntoCommand";
  private static final String MERGE_INTO_COMMAND_EDGE_SUFFIX = ".MergeIntoCommandEdge";

  private final OpenLineageContext context;

  private static final List<String> DELTA_INTERNAL_RDD_COLUMNS =
      Arrays.asList("txn", "add", "remove", "metaData", "cdc", "protocol", "commitInfo");

  private static final List<String> DELTA_LOG_INTERNAL_COLUMNS =
      Arrays.asList("protocol", "metaData", "action_sort_column");

  private static final String STAGING_DELTA_TABLE =
      "AppendDataExecV1 org.apache.spark.sql.delta.catalog.DeltaCatalog$StagedDeltaTableV2";

  public DeltaEventFilter(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean isDisabled(SparkListenerEvent event) {
    if (!isDeltaPlan()) {
      return false;
    }

    if (isMergeIntoCommand()) {
      log.debug("Allowing Delta MERGE event through the OpenLineage filter");
      return false;
    }

    return isFilterRoot()
        || isLocalRelationOnly()
        || isLogicalRDDWithInternalDataColumns()
        || isStagedDeltaTable(event)
        || isDeltaLogProjection()
        || isSerializeFromObject()
        || isOnJobStartOrEnd(event);
  }

  private boolean isMergeIntoCommand() {
    return getLogicalPlan(context)
        .map(LogicalPlan::getClass)
        .map(Class::getName)
        .map(DeltaEventFilter::isMergeIntoCommandClass)
        .orElse(false);
  }

  static boolean isMergeIntoCommandClass(String className) {
    return className.endsWith(MERGE_INTO_COMMAND_SUFFIX)
        || className.endsWith(MERGE_INTO_COMMAND_EDGE_SUFFIX);
  }

  /**
   * We get exact copies of OL events for org.apache.spark.scheduler.SparkListenerJobStart and
   * org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart. The same happens for end
   * events.
   */
  private boolean isOnJobStartOrEnd(SparkListenerEvent event) {
    return event instanceof SparkListenerJobStart || event instanceof SparkListenerJobEnd;
  }

  /** Returns true if Staged Delta table is written */
  private boolean isStagedDeltaTable(SparkListenerEvent event) {
    if (event instanceof SparkListenerSQLExecutionStart) {
      return Optional.of((SparkListenerSQLExecutionStart) event)
          .map(SparkListenerSQLExecutionStart::sparkPlanInfo)
          .map(SparkPlanInfo::simpleString)
          .filter(s -> s.contains(STAGING_DELTA_TABLE))
          .isPresent();
    } else if (event instanceof SparkListenerSQLExecutionEnd) {
      return Optional.of((SparkListenerSQLExecutionEnd) event)
          .map(SparkListenerSQLExecutionEnd::qe)
          .map(QueryExecution::executedPlan)
          .map(TreeNode::toString)
          .filter(s -> s.contains(STAGING_DELTA_TABLE))
          .isPresent();
    }
    return false;
  }

  /** Returns true if LocalRelation is the only element of LogicalPlan. */
  private boolean isLocalRelationOnly() {
    return getLogicalPlan(context)
        .filter(plan -> plan.children() != null)
        .filter(plan -> plan.children().isEmpty())
        .filter(plan -> plan instanceof LocalRelation)
        .isPresent();
  }

  /** Returns true if Filter is a root node of LogicalPlan */
  private boolean isFilterRoot() {
    return getLogicalPlan(context).filter(plan -> plan instanceof Filter).isPresent();
  }

  private boolean isDeltaLogProjection() {
    return getLogicalPlan(context)
        .filter(plan -> plan instanceof Project)
        .map(project -> JavaConverters.seqAsJavaListConverter(project.output()).asJava())
        .map(
            attributes ->
                attributes.stream()
                    .map(NamedExpression::name)
                    .collect(Collectors.toSet())
                    .containsAll(DELTA_LOG_INTERNAL_COLUMNS))
        .orElse(false);
  }

  /**
   * Delta internally performs actions on 'LogicalRDD [txn#92, add#93, remove#94, metaData#95,
   * protocol#96, cdc#97, commitInfo#98]'. If the leaf of logical plan is LogicalRDD with such
   * columns, we disable OL event.
   */
  private boolean isLogicalRDDWithInternalDataColumns() {
    return getLogicalPlan(context)
        .map(
            plan ->
                JavaConverters.seqAsJavaListConverter(plan.collectLeaves()).asJava().stream()
                    .filter(node -> node instanceof LogicalRDD)
                    .map(node -> (LogicalRDD) node)
                    .map(node -> JavaConverters.seqAsJavaListConverter(node.output()).asJava())
                    .map(
                        attributes ->
                            attributes.stream()
                                .map(NamedExpression::name)
                                .collect(Collectors.toSet()))
                    .anyMatch(attrs -> attrs.containsAll(DELTA_INTERNAL_RDD_COLUMNS)))
        .orElse(false);
  }

  private boolean isSerializeFromObject() {
    return getLogicalPlan(context).map(plan -> plan instanceof SerializeFromObject).orElse(false);
  }
}
