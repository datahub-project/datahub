package com.linkedin.datahub.graphql.types.ownership;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.Status;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.GetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AuditStamp;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.OwnershipTypeEntity;
import com.linkedin.datahub.graphql.generated.OwnershipTypeInfo;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class OwnershipTypeMapper implements ModelMapper<EntityResponse, OwnershipTypeEntity> {

  public static final OwnershipTypeMapper INSTANCE = new OwnershipTypeMapper();

  public static OwnershipTypeEntity map(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public OwnershipTypeEntity apply(@Nullable QueryContext context, @Nonnull EntityResponse input) {
    final OwnershipTypeEntity result = new OwnershipTypeEntity();

    result.setUrn(input.getUrn().toString());
    result.setType(EntityType.CUSTOM_OWNERSHIP_TYPE);
    EnvelopedAspectMap aspectMap = input.getAspects();
    MappingHelper<OwnershipTypeEntity> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(OWNERSHIP_TYPE_INFO_ASPECT_NAME, this::mapOwnershipTypeInfo);
    mappingHelper.mapToResult(
        STATUS_ASPECT_NAME,
        (dataset, dataMap) -> dataset.setStatus(StatusMapper.map(context, new Status(dataMap))));
    return mappingHelper.getResult();
  }

  private void mapOwnershipTypeInfo(
      @Nonnull OwnershipTypeEntity ownershipTypeEntity, @Nonnull DataMap dataMap) {
    final com.linkedin.ownership.OwnershipTypeInfo gmsOwnershipTypeInfo =
        new com.linkedin.ownership.OwnershipTypeInfo(dataMap);

    final OwnershipTypeInfo ownershipTypeInfo = new OwnershipTypeInfo();

    ownershipTypeInfo.setName(gmsOwnershipTypeInfo.getName(GetMode.NULL));
    ownershipTypeInfo.setDescription(gmsOwnershipTypeInfo.getDescription(GetMode.NULL));

    AuditStamp created = new AuditStamp();
    created.setTime(gmsOwnershipTypeInfo.getCreated().getTime());
    created.setActor(gmsOwnershipTypeInfo.getCreated().getActor(GetMode.NULL).toString());
    ownershipTypeInfo.setCreated(created);

    AuditStamp lastModified = new AuditStamp();
    lastModified.setTime(gmsOwnershipTypeInfo.getCreated().getTime());
    lastModified.setActor(gmsOwnershipTypeInfo.getCreated().getActor(GetMode.NULL).toString());
    ownershipTypeInfo.setLastModified(lastModified);

    ownershipTypeEntity.setInfo(ownershipTypeInfo);
  }
}
