package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.common.VersionTag;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class VersionTagMapper
    implements ModelMapper<VersionTag, com.linkedin.datahub.graphql.generated.VersionTag> {
  public static final VersionTagMapper INSTANCE = new VersionTagMapper();

  public static com.linkedin.datahub.graphql.generated.VersionTag map(
      @Nullable QueryContext context, @Nonnull final VersionTag versionTag) {
    return INSTANCE.apply(context, versionTag);
  }

  @Override
  public com.linkedin.datahub.graphql.generated.VersionTag apply(
      @Nullable QueryContext context, @Nonnull final VersionTag input) {
    final com.linkedin.datahub.graphql.generated.VersionTag result =
        new com.linkedin.datahub.graphql.generated.VersionTag();
    result.setVersionTag(input.getVersionTag());
    return result;
  }
}
