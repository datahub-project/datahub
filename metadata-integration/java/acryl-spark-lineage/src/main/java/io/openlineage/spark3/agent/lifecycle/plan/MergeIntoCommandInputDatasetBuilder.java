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
    super(context, true);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof MergeIntoCommand;
  }

  @Override
  protected List<OpenLineage.InputDataset> apply(SparkListenerEvent event, MergeIntoCommand x) {
    List<OpenLineage.InputDataset> datasets = new ArrayList<>();
    datasets.addAll(delegate(x.target(), event));
    datasets.addAll(delegate(x.source(), event));

    return datasets;
  }
}
