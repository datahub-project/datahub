package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.generated.Status;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;

public class StatusMapper implements ModelMapper<com.linkedin.common.Status, Status> {

  public static final StatusMapper INSTANCE = new StatusMapper();

  public static Status map(@Nonnull final com.linkedin.common.Status metadata) {
    return INSTANCE.apply(metadata);
  }

  @Override
  public Status apply(@Nonnull final com.linkedin.common.Status input) {
    final Status result = new Status();
    result.setRemoved(input.isRemoved());
    return result;
  }
}
