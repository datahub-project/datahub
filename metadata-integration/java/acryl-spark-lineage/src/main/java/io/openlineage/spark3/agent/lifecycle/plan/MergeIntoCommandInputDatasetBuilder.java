/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.delta.commands.MergeIntoCommand;

@Slf4j
public class MergeIntoCommandInputDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<MergeIntoCommand> {

  public MergeIntoCommandInputDatasetBuilder(OpenLineageContext context) {
    super(context, true); // FIXED: This enables recursive traversal of subqueries
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof MergeIntoCommand;
  }

  @Override
  protected List<OpenLineage.InputDataset> apply(SparkListenerEvent event, MergeIntoCommand x) {
    List<OpenLineage.InputDataset> datasets = new ArrayList<>();

    // Process target table
    List<OpenLineage.InputDataset> targetDatasets = delegate(x.target(), event);
    datasets.addAll(targetDatasets);

    // Process source - this will recursively process all datasets in the source plan,
    // including those in subqueries
    List<OpenLineage.InputDataset> sourceDatasets = delegate(x.source(), event);
    datasets.addAll(sourceDatasets);

    // Handle complex subqueries that aren't captured by standard delegation
    if (sourceDatasets.isEmpty()) {
      sourceDatasets.addAll(extractInputDatasetsFromComplexSource(x.source(), event));
      datasets.addAll(sourceDatasets);
    }

    return datasets;
  }

  /**
   * Extracts input datasets from complex source plans like subqueries with DISTINCT, PROJECT, etc.
   * This handles cases where the standard delegation doesn't work due to missing builders for
   * intermediate logical plan nodes.
   */
  private List<OpenLineage.InputDataset> extractInputDatasetsFromComplexSource(
      LogicalPlan source, SparkListenerEvent event) {
    List<OpenLineage.InputDataset> datasets = new ArrayList<>();

    // Use a queue to traverse the logical plan tree depth-first
    java.util.Queue<LogicalPlan> queue = new java.util.LinkedList<>();
    queue.offer(source);

    while (!queue.isEmpty()) {
      LogicalPlan current = queue.poll();

      // Try to delegate this node directly
      List<OpenLineage.InputDataset> currentDatasets = delegate(current, event);
      datasets.addAll(currentDatasets);

      // If this node didn't produce any datasets, traverse its children
      if (currentDatasets.isEmpty()) {
        // Add all children to the queue for traversal
        scala.collection.Iterator<LogicalPlan> children = current.children().iterator();
        while (children.hasNext()) {
          queue.offer(children.next());
        }
      }
    }

    return datasets;
  }
}
