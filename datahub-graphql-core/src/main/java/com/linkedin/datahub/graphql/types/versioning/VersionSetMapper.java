package com.linkedin.datahub.graphql.types.versioning;

import static com.linkedin.metadata.Constants.VERSION_SET_PROPERTIES_ASPECT_NAME;

import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.VersionSet;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class VersionSetMapper implements ModelMapper<EntityResponse, VersionSet> {

  public static final VersionSetMapper INSTANCE = new VersionSetMapper();

  public static VersionSet map(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public VersionSet apply(@Nullable QueryContext context, @Nonnull EntityResponse entityResponse) {
    final VersionSet result = new VersionSet();
    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.VERSION_SET);

    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<VersionSet> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(
        VERSION_SET_PROPERTIES_ASPECT_NAME,
        (versionSet, dataMap) -> mapVersionSetProperties(context, versionSet, dataMap));

    return result;
  }

  private void mapVersionSetProperties(
      @Nullable QueryContext context, @Nonnull VersionSet versionSet, @Nonnull DataMap dataMap) {
    com.linkedin.versionset.VersionSetProperties versionProperties =
        new com.linkedin.versionset.VersionSetProperties(dataMap);
    versionSet.setLatestVersion(UrnToEntityMapper.map(context, versionProperties.getLatest()));
  }
}
