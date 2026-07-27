/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark34.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.ReflectionUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.streaming.FileStreamSink;
import org.apache.spark.sql.execution.streaming.Sink;
import org.apache.spark.sql.execution.streaming.sources.WriteToMicroBatchDataSourceV1;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;

/**
 * {@link LogicalPlan} visitor that matches {@link WriteToMicroBatchDataSourceV1} commands and
 * extracts the output {@link OpenLineage.Dataset} being written to micro batch data sources using
 * the V1 API.
 */
@Slf4j
public class WriteToMicroBatchDataSourceV1DatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<WriteToMicroBatchDataSourceV1> {

  private static final String DELTA_SINK_CLASS_NAME =
      "org.apache.spark.sql.delta.sources.DeltaSink";

  private final DatasetFactory<OpenLineage.OutputDataset> factory;

  public WriteToMicroBatchDataSourceV1DatasetBuilder(
      OpenLineageContext context, DatasetFactory<OpenLineage.OutputDataset> factory) {
    super(context, false);
    this.factory = factory;
  }

  @Override
  public boolean isDefinedAt(SparkListenerEvent event) {
    if (!(event instanceof SparkListenerSQLExecutionEnd)) {
      return false;
    }
    SparkListenerSQLExecutionEnd see = (SparkListenerSQLExecutionEnd) event;
    return isDefinedAtLogicalPlan(see.qe().analyzed());
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan logicalPlan) {
    return logicalPlan instanceof WriteToMicroBatchDataSourceV1;
  }

  @Override
  protected List<OpenLineage.OutputDataset> apply(
      SparkListenerEvent event, WriteToMicroBatchDataSourceV1 writeToMicroBatchV1) {
    Sink sink = writeToMicroBatchV1.sink();
    String sinkClassName = sink.getClass().getCanonicalName();
    boolean fileSink = sink instanceof FileStreamSink;
    boolean deltaSink = DELTA_SINK_CLASS_NAME.equals(sinkClassName);

    if (!fileSink && !deltaSink) {
      log.debug("Unsupported Sink type: {}", sink.getClass().getName());
      return Collections.emptyList();
    }

    if (writeToMicroBatchV1.catalogTable().isDefined()) {
      return Collections.singletonList(
          factory
              .sparkDatasetBuilder()
              .dataset(writeToMicroBatchV1.catalogTable().get())
              .schema(writeToMicroBatchV1.schema())
              .build());
    }

    if (deltaSink) {
      return deltaPath(sink)
          .map(
              path ->
                  Collections.singletonList(
                      factory
                          .sparkDatasetBuilder()
                          .dataset(path)
                          .schema(writeToMicroBatchV1.schema())
                          .build()))
          .orElseGet(Collections::emptyList);
    }

    return Collections.emptyList();
  }

  private Optional<URI> deltaPath(Sink sink) {
    try {
      return ReflectionUtils.tryExecuteMethod(sink, "path").map(Object::toString).map(URI::create);
    } catch (IllegalArgumentException e) {
      log.warn("Could not extract a valid path from {}", sink.getClass().getName(), e);
      return Optional.empty();
    }
  }
}
