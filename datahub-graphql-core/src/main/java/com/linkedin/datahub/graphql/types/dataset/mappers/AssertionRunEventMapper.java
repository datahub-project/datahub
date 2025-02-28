package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.datahub.graphql.types.assertion.AssertionMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.datahub.graphql.types.mappers.TimeSeriesAspectMapper;
import com.linkedin.datahub.graphql.types.monitor.MonitorMapper;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.utils.GenericRecordUtils;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class AssertionRunEventMapper
    implements TimeSeriesAspectMapper<com.linkedin.datahub.graphql.generated.AssertionRunEvent> {

  public static final AssertionRunEventMapper INSTANCE = new AssertionRunEventMapper();

  public static com.linkedin.datahub.graphql.generated.AssertionRunEvent map(
      @Nullable QueryContext context, @Nonnull final EnvelopedAspect envelopedAspect) {
    return INSTANCE.apply(context, envelopedAspect);
  }

  public static AssertionResult mapResult(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.assertion.AssertionResult gmsResult) {
    return INSTANCE.mapAssertionResult(context, gmsResult);
  }

  @Override
  public com.linkedin.datahub.graphql.generated.AssertionRunEvent apply(
      @Nullable QueryContext context, @Nonnull final EnvelopedAspect envelopedAspect) {

    AssertionRunEvent gmsAssertionRunEvent =
        GenericRecordUtils.deserializeAspect(
            envelopedAspect.getAspect().getValue(),
            envelopedAspect.getAspect().getContentType(),
            AssertionRunEvent.class);

    final com.linkedin.datahub.graphql.generated.AssertionRunEvent assertionRunEvent =
        new com.linkedin.datahub.graphql.generated.AssertionRunEvent();

    if (envelopedAspect.hasSystemMetadata()
        && envelopedAspect.getSystemMetadata().hasLastObserved()) {
      assertionRunEvent.setLastObservedMillis(
          envelopedAspect.getSystemMetadata().getLastObserved());
    }
    assertionRunEvent.setTimestampMillis(gmsAssertionRunEvent.getTimestampMillis());
    assertionRunEvent.setAssertionUrn(gmsAssertionRunEvent.getAssertionUrn().toString());
    assertionRunEvent.setAsserteeUrn(gmsAssertionRunEvent.getAsserteeUrn().toString());
    assertionRunEvent.setRunId(gmsAssertionRunEvent.getRunId());
    assertionRunEvent.setStatus(
        AssertionRunStatus.valueOf(gmsAssertionRunEvent.getStatus().name()));
    if (gmsAssertionRunEvent.hasBatchSpec()) {
      assertionRunEvent.setBatchSpec(mapBatchSpec(context, gmsAssertionRunEvent.getBatchSpec()));
    }
    if (gmsAssertionRunEvent.hasPartitionSpec()) {
      assertionRunEvent.setPartitionSpec(mapPartitionSpec(gmsAssertionRunEvent.getPartitionSpec()));
    }
    if (gmsAssertionRunEvent.hasResult()) {
      assertionRunEvent.setResult(mapAssertionResult(context, gmsAssertionRunEvent.getResult()));
    }
    if (gmsAssertionRunEvent.hasRuntimeContext()) {
      assertionRunEvent.setRuntimeContext(
          StringMapMapper.map(context, gmsAssertionRunEvent.getRuntimeContext()));
    }

    return assertionRunEvent;
  }

  private PartitionSpec mapPartitionSpec(com.linkedin.timeseries.PartitionSpec gmsPartitionSpec) {
    PartitionSpec partitionSpec = new PartitionSpec();
    partitionSpec.setPartition(gmsPartitionSpec.getPartition());
    partitionSpec.setType(PartitionType.valueOf(gmsPartitionSpec.getType().name()));
    return partitionSpec;
  }

  private AssertionResult mapAssertionResult(
      @Nullable QueryContext context, com.linkedin.assertion.AssertionResult gmsResult) {
    AssertionResult datasetAssertionResult = new AssertionResult();
    datasetAssertionResult.setRowCount(gmsResult.getRowCount());
    datasetAssertionResult.setActualAggValue(gmsResult.getActualAggValue());
    datasetAssertionResult.setMissingCount(gmsResult.getMissingCount());
    datasetAssertionResult.setUnexpectedCount(gmsResult.getUnexpectedCount());
    datasetAssertionResult.setExternalUrl(gmsResult.getExternalUrl());

    if (gmsResult.hasAssertion()) {
      AssertionInfo datasetAssertionInfo =
          AssertionMapper.mapAssertionInfo(context, gmsResult.getAssertion());
      datasetAssertionResult.setAssertion(datasetAssertionInfo);
    }

    if (gmsResult.hasParameters()) {
      AssertionEvaluationParameters datasetAssertionEvaluationParameters =
          MonitorMapper.mapAssertionEvaluationParameters(gmsResult.getParameters());
      datasetAssertionResult.setParameters(datasetAssertionEvaluationParameters);
    }

    if (gmsResult.hasAssertionInferenceDetails()) {
      datasetAssertionResult.setAssertionInferenceDetails(
          AssertionMapper.mapInferenceDetails(context, gmsResult.getAssertionInferenceDetails()));
    }

    if (gmsResult.hasType()) {
      AssertionResultType assertionType = AssertionResultType.valueOf(gmsResult.getType().name());
      datasetAssertionResult.setType(assertionType);
    }

    if (gmsResult.hasNativeResults()) {
      datasetAssertionResult.setNativeResults(
          StringMapMapper.map(context, gmsResult.getNativeResults()));
    }

    if (gmsResult.hasError()) {
      datasetAssertionResult.setError(mapAssertionResultError(context, gmsResult.getError()));
    }

    return datasetAssertionResult;
  }

  private AssertionResultError mapAssertionResultError(
      @Nullable final QueryContext context, com.linkedin.assertion.AssertionResultError gmsResult) {
    AssertionResultError datasetAssertionResultError = new AssertionResultError();
    datasetAssertionResultError.setType(
        AssertionResultErrorType.valueOf(gmsResult.getType().name()));

    if (gmsResult.hasProperties()) {
      datasetAssertionResultError.setProperties(
          StringMapMapper.map(context, gmsResult.getProperties()));
    }
    return datasetAssertionResultError;
  }

  private BatchSpec mapBatchSpec(
      @Nullable QueryContext context, com.linkedin.assertion.BatchSpec gmsBatchSpec) {
    BatchSpec batchSpec = new BatchSpec();
    batchSpec.setNativeBatchId(gmsBatchSpec.getNativeBatchId());
    batchSpec.setLimit(gmsBatchSpec.getLimit());
    batchSpec.setQuery(gmsBatchSpec.getQuery());
    batchSpec.setCustomProperties(StringMapMapper.map(context, gmsBatchSpec.getCustomProperties()));
    return batchSpec;
  }
}
