package com.linkedin.datahub.graphql.types.versioning;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.VersionProperties;
import com.linkedin.datahub.graphql.generated.VersionSet;
import com.linkedin.datahub.graphql.types.mappers.MapperUtils;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.VersionTagMapper;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class VersionPropertiesMapper
    implements ModelMapper<com.linkedin.common.VersionProperties, VersionProperties> {
  public static final VersionPropertiesMapper INSTANCE = new VersionPropertiesMapper();

  public static VersionProperties map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.common.VersionProperties versionProperties) {
    return INSTANCE.apply(context, versionProperties);
  }

  @Override
  public VersionProperties apply(
      @Nullable QueryContext context, @Nonnull com.linkedin.common.VersionProperties input) {
    final VersionProperties result = new VersionProperties();

    result.setVersionSet(
        VersionSet.builder()
            .setUrn(input.getVersionSet().toString())
            .setType(EntityType.VERSION_SET)
            .build());

    result.setVersion(VersionTagMapper.map(context, input.getVersion()));
    result.setAliases(
        input.getAliases().stream()
            .map(alias -> VersionTagMapper.map(context, alias))
            .collect(Collectors.toList()));
    result.setComment(input.getComment());
    result.setIsLatest(Boolean.TRUE.equals(input.isIsLatest()));

    if (input.getMetadataCreatedTimestamp() != null) {
      result.setCreated(MapperUtils.createResolvedAuditStamp(input.getMetadataCreatedTimestamp()));
    }
    if (input.getSourceCreatedTimestamp() != null) {
      result.setCreatedInSource(
          MapperUtils.createResolvedAuditStamp(input.getSourceCreatedTimestamp()));
    }

    return result;
  }
}
