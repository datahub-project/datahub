package com.linkedin.datahub.graphql.types.dimension;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.Status;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.GetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AuditStamp;
import com.linkedin.datahub.graphql.generated.DimensionNameEntity;
import com.linkedin.datahub.graphql.generated.DimensionNameInfo;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DimensionNameMapper implements ModelMapper<EntityResponse, DimensionNameEntity> {

  public static final DimensionNameMapper INSTANCE = new DimensionNameMapper();

  public static DimensionNameEntity map(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public DimensionNameEntity apply(@Nullable QueryContext context, @Nonnull EntityResponse input) {
    final DimensionNameEntity result = new DimensionNameEntity();

    result.setUrn(input.getUrn().toString());
    result.setType(EntityType.DIMENSION_NAME);
    EnvelopedAspectMap aspectMap = input.getAspects();
    MappingHelper<DimensionNameEntity> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(DIMENSION_NAME_INFO_ASPECT_NAME, this::mapDimensionNameInfo);
    mappingHelper.mapToResult(
        STATUS_ASPECT_NAME,
        (dataset, dataMap) -> dataset.setStatus(StatusMapper.map(context, new Status(dataMap))));
    return mappingHelper.getResult();
  }

  private void mapDimensionNameInfo(
      @Nonnull DimensionNameEntity dimensionNameEntity, @Nonnull DataMap dataMap) {
    final com.linkedin.dataquality.DimensionTypeInfo gmsDimensionTypeInfo =
        new com.linkedin.dataquality.DimensionTypeInfo(dataMap);

    final DimensionNameInfo dimensionNameInfo = new DimensionNameInfo();

    dimensionNameInfo.setName(gmsDimensionTypeInfo.getName(GetMode.NULL));
    dimensionNameInfo.setDescription(gmsDimensionTypeInfo.getDescription(GetMode.NULL));

    AuditStamp created = new AuditStamp();
    created.setTime(gmsDimensionTypeInfo.getCreated().getTime());
    created.setActor(gmsDimensionTypeInfo.getCreated().getActor(GetMode.NULL).toString());
    dimensionNameInfo.setCreated(created);

    AuditStamp lastModified = new AuditStamp();
    lastModified.setTime(gmsDimensionTypeInfo.getCreated().getTime());
    lastModified.setActor(gmsDimensionTypeInfo.getCreated().getActor(GetMode.NULL).toString());
    dimensionNameInfo.setLastModified(lastModified);

    dimensionNameEntity.setInfo(dimensionNameInfo);
  }
}
