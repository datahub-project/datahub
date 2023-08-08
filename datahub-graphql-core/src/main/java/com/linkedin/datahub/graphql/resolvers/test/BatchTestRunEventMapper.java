package com.linkedin.datahub.graphql.resolvers.test;

import com.linkedin.datahub.graphql.generated.BatchTestRunResult;
import com.linkedin.datahub.graphql.generated.BatchTestRunStatus;
import com.linkedin.datahub.graphql.types.mappers.TimeSeriesAspectMapper;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.test.BatchTestRunEvent;
import javax.annotation.Nonnull;


public class BatchTestRunEventMapper
    implements TimeSeriesAspectMapper<com.linkedin.datahub.graphql.generated.BatchTestRunEvent> {

  public static final BatchTestRunEventMapper INSTANCE = new BatchTestRunEventMapper();

  public static com.linkedin.datahub.graphql.generated.BatchTestRunEvent map(
      @Nonnull final EnvelopedAspect envelopedAspect) {
    return INSTANCE.apply(envelopedAspect);
  }

  @Override
  public com.linkedin.datahub.graphql.generated.BatchTestRunEvent apply(
      @Nonnull final EnvelopedAspect envelopedAspect) {

    BatchTestRunEvent gmsRunEvent =
        GenericRecordUtils.deserializeAspect(envelopedAspect.getAspect().getValue(),
            envelopedAspect.getAspect().getContentType(), BatchTestRunEvent.class);

    final com.linkedin.datahub.graphql.generated.BatchTestRunEvent runEvent =
        new com.linkedin.datahub.graphql.generated.BatchTestRunEvent();

    runEvent.setTimestampMillis(gmsRunEvent.getTimestampMillis());
    runEvent.setStatus(BatchTestRunStatus.valueOf(gmsRunEvent.getStatus().toString()));
    if (gmsRunEvent.hasResult()) {
      runEvent.setResult(mapResult(gmsRunEvent.getResult()));
    }
    return runEvent;
  }

  private BatchTestRunResult mapResult(com.linkedin.test.BatchTestRunResult gmsResult) {
    BatchTestRunResult result = new BatchTestRunResult();
    result.setFailingCount(gmsResult.getFailingCount());
    result.setPassingCount(gmsResult.getPassingCount());
    return result;
  }
}
