package com.linkedin.datahub.graphql.types.dataprocessinst.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.types.mappers.TimeSeriesAspectMapper;
import com.linkedin.dataprocess.DataProcessInstanceRunEvent;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.utils.GenericRecordUtils;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DataProcessInstanceRunEventMapper
    implements TimeSeriesAspectMapper<com.linkedin.datahub.graphql.generated.DataProcessRunEvent> {

  public static final DataProcessInstanceRunEventMapper INSTANCE =
      new DataProcessInstanceRunEventMapper();

  public static com.linkedin.datahub.graphql.generated.DataProcessRunEvent map(
      @Nullable QueryContext context, @Nonnull final EnvelopedAspect envelopedAspect) {
    return INSTANCE.apply(context, envelopedAspect);
  }

  @Override
  public com.linkedin.datahub.graphql.generated.DataProcessRunEvent apply(
      @Nullable QueryContext context, @Nonnull final EnvelopedAspect envelopedAspect) {

    DataProcessInstanceRunEvent runEvent =
        GenericRecordUtils.deserializeAspect(
            envelopedAspect.getAspect().getValue(),
            envelopedAspect.getAspect().getContentType(),
            DataProcessInstanceRunEvent.class);

    final com.linkedin.datahub.graphql.generated.DataProcessRunEvent result =
        new com.linkedin.datahub.graphql.generated.DataProcessRunEvent();

    result.setTimestampMillis(runEvent.getTimestampMillis());
    result.setAttempt(runEvent.getAttempt());
    if (runEvent.hasStatus()) {
      result.setStatus(
          com.linkedin.datahub.graphql.generated.DataProcessRunStatus.valueOf(
              runEvent.getStatus().toString()));
    }
    if (runEvent.hasResult()) {
      result.setResult(DataProcessInstanceRunResultMapper.map(context, runEvent.getResult()));
    }
    if (runEvent.hasDurationMillis()) {
      result.setDurationMillis(runEvent.getDurationMillis());
    }

    return result;
  }
}
