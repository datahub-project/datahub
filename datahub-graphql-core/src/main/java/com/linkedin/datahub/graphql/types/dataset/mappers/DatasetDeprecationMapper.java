package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Deprecation;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DatasetDeprecationMapper
    implements ModelMapper<com.linkedin.dataset.DatasetDeprecation, Deprecation> {

  public static final DatasetDeprecationMapper INSTANCE = new DatasetDeprecationMapper();

  public static Deprecation map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.dataset.DatasetDeprecation deprecation) {
    return INSTANCE.apply(context, deprecation);
  }

  @Override
  public Deprecation apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.dataset.DatasetDeprecation input) {
    final Deprecation result = new Deprecation();
    result.setActor(input.getActor().toString());
    result.setDeprecated(input.isDeprecated());
    result.setDecommissionTime(input.getDecommissionTime());
    result.setNote(input.getNote());
    return result;
  }
}
