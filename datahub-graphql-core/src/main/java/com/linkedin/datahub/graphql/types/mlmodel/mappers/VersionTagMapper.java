package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.common.VersionTag;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;

public class VersionTagMapper
    implements ModelMapper<VersionTag, com.linkedin.datahub.graphql.generated.VersionTag> {
  public static final VersionTagMapper INSTANCE = new VersionTagMapper();

  public static com.linkedin.datahub.graphql.generated.VersionTag map(
      @Nonnull final VersionTag versionTag) {
    return INSTANCE.apply(versionTag);
  }

  @Override
  public com.linkedin.datahub.graphql.generated.VersionTag apply(@Nonnull final VersionTag input) {
    final com.linkedin.datahub.graphql.generated.VersionTag result =
        new com.linkedin.datahub.graphql.generated.VersionTag();
    result.setVersionTag(input.getVersionTag());
    return result;
  }
}
