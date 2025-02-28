package com.linkedin.datahub.graphql.types.entitytype;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.EntityTypeEntity;
import com.linkedin.datahub.graphql.generated.EntityTypeInfo;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class EntityTypeEntityMapper implements ModelMapper<EntityResponse, EntityTypeEntity> {

  public static final EntityTypeEntityMapper INSTANCE = new EntityTypeEntityMapper();

  public static EntityTypeEntity map(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public EntityTypeEntity apply(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final EntityTypeEntity result = new EntityTypeEntity();
    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.ENTITY_TYPE);
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<EntityTypeEntity> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(ENTITY_TYPE_INFO_ASPECT_NAME, this::mapEntityTypeInfo);

    // Set the standard Type ENUM for the entity type.
    if (result.getInfo() != null) {
      result
          .getInfo()
          .setType(EntityTypeUrnMapper.getEntityType(entityResponse.getUrn().toString()));
    }
    return mappingHelper.getResult();
  }

  private void mapEntityTypeInfo(@Nonnull EntityTypeEntity entityType, @Nonnull DataMap dataMap) {
    com.linkedin.entitytype.EntityTypeInfo gmsInfo =
        new com.linkedin.entitytype.EntityTypeInfo(dataMap);
    EntityTypeInfo info = new EntityTypeInfo();
    info.setQualifiedName(gmsInfo.getQualifiedName());
    if (gmsInfo.getDisplayName() != null) {
      info.setDisplayName(gmsInfo.getDisplayName());
    }
    if (gmsInfo.getDescription() != null) {
      info.setDescription(gmsInfo.getDescription());
    }
    entityType.setInfo(info);
  }
}
