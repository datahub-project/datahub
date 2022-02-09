package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.datahub.graphql.generated.AssertionResult;
import com.linkedin.datahub.graphql.generated.AssertionResultType;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.datahub.graphql.types.mappers.TimeSeriesAspectMapper;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.utils.GenericAspectUtils;
import javax.annotation.Nonnull;


public class DatasetAssertionMapper
    implements TimeSeriesAspectMapper<com.linkedin.datahub.graphql.generated.AssertionRunEvent> {

  public static final DatasetAssertionMapper INSTANCE = new DatasetAssertionMapper();

  public static com.linkedin.datahub.graphql.generated.AssertionRunEvent map(
      @Nonnull final EnvelopedAspect envelopedAspect) {
    return INSTANCE.apply(envelopedAspect);
  }

  @Override
  public com.linkedin.datahub.graphql.generated.AssertionRunEvent apply(
      @Nonnull final EnvelopedAspect envelopedAspect) {

    AssertionRunEvent gmsAssertionRunEvent =
        GenericAspectUtils.deserializeAspect(envelopedAspect.getAspect().getValue(),
            envelopedAspect.getAspect().getContentType(), AssertionRunEvent.class);

    final com.linkedin.datahub.graphql.generated.AssertionRunEvent datasetAssertion =
        new com.linkedin.datahub.graphql.generated.AssertionRunEvent();

    datasetAssertion.setTimestampMillis(gmsAssertionRunEvent.getTimestampMillis());
    datasetAssertion.setAssertionUrn(gmsAssertionRunEvent.getAssertionUrn().toString());
    if (gmsAssertionRunEvent.hasResult()) {
      AssertionResult datasetAssertionResult = new AssertionResult();
      datasetAssertionResult.setRowCount(gmsAssertionRunEvent.getResult().getRowCount());
      datasetAssertionResult.setActualAggValue(gmsAssertionRunEvent.getResult().getActualAggValue());
      datasetAssertionResult.setMissingCount(gmsAssertionRunEvent.getResult().getMissingCount());
      datasetAssertionResult.setUnexpectedCount(gmsAssertionRunEvent.getResult().getUnexpectedCount());
      datasetAssertionResult.setExternalUrl(gmsAssertionRunEvent.getResult().getExternalUrl());
      if (gmsAssertionRunEvent.getResult().hasType()) {
        AssertionResultType assertionType =
            AssertionResultType.valueOf(gmsAssertionRunEvent.getResult().getType().name());
        datasetAssertionResult.setType(assertionType);
      }
      datasetAssertion.setResult(datasetAssertionResult);
      if (gmsAssertionRunEvent.getResult().hasNativeResults()) {
        datasetAssertionResult.setNativeResults(
            StringMapMapper.map(gmsAssertionRunEvent.getResult().getNativeResults()));
      }
      if (gmsAssertionRunEvent.getResult().hasRuntimeContext()) {
        datasetAssertionResult.setRuntimeContext(
            StringMapMapper.map(gmsAssertionRunEvent.getResult().getRuntimeContext()));
      }
    }

    return datasetAssertion;
  }
}
