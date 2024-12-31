package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.OwnershipSource;
import com.linkedin.datahub.graphql.generated.OwnershipSourceType;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class OwnershipSourceMapper
    implements ModelMapper<com.linkedin.common.OwnershipSource, OwnershipSource> {

  public static final OwnershipSourceMapper INSTANCE = new OwnershipSourceMapper();

  public static OwnershipSource map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.common.OwnershipSource ownershipSource) {
    return INSTANCE.apply(context, ownershipSource);
  }

  @Override
  public OwnershipSource apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.common.OwnershipSource ownershipSource) {
    final OwnershipSource result = new OwnershipSource();
    result.setUrl(ownershipSource.getUrl());
    result.setType(Enum.valueOf(OwnershipSourceType.class, ownershipSource.getType().toString()));
    return result;
  }
}
