package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.TimeStamp;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class TimeStampMapper implements ModelMapper<com.linkedin.common.TimeStamp, TimeStamp> {

  public static final TimeStampMapper INSTANCE = new TimeStampMapper();

  public static TimeStamp map(
      @Nullable QueryContext context, @Nonnull final com.linkedin.common.TimeStamp timestamp) {
    return INSTANCE.apply(context, timestamp);
  }

  @Override
  public TimeStamp apply(
      @Nullable QueryContext context, @Nonnull final com.linkedin.common.TimeStamp timestamp) {
    final TimeStamp result = new TimeStamp();
    result.setTime(timestamp.getTime());
    if (timestamp.hasActor()) {
      result.setActor(timestamp.getActor().toString());
    }
    return result;
  }
}
