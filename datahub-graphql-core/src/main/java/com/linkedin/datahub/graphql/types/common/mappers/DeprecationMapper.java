package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.generated.Deprecation;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;

public class DeprecationMapper
    implements ModelMapper<com.linkedin.common.Deprecation, Deprecation> {
  public static final DeprecationMapper INSTANCE = new DeprecationMapper();

  public static Deprecation map(@Nonnull final com.linkedin.common.Deprecation deprecation) {
    return INSTANCE.apply(deprecation);
  }

  @Override
  public Deprecation apply(@Nonnull final com.linkedin.common.Deprecation input) {
    final Deprecation result = new Deprecation();
    result.setActor(input.getActor().toString());
    result.setDeprecated(input.isDeprecated());
    result.setDecommissionTime(input.getDecommissionTime());
    result.setNote(input.getNote());
    return result;
  }
}
