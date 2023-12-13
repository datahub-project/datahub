package com.linkedin.datahub.graphql.types.role.mappers;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.DataHubRole;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.policy.DataHubRoleInfo;
import javax.annotation.Nonnull;

public class DataHubRoleMapper implements ModelMapper<EntityResponse, DataHubRole> {

  public static final DataHubRoleMapper INSTANCE = new DataHubRoleMapper();

  public static DataHubRole map(@Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(entityResponse);
  }

  @Override
  public DataHubRole apply(@Nonnull final EntityResponse entityResponse) {
    final DataHubRole result = new DataHubRole();

    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.DATAHUB_ROLE);
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<DataHubRole> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(DATAHUB_ROLE_INFO_ASPECT_NAME, this::mapDataHubRoleInfo);
    return mappingHelper.getResult();
  }

  private void mapDataHubRoleInfo(@Nonnull DataHubRole role, @Nonnull DataMap dataMap) {
    DataHubRoleInfo roleInfo = new DataHubRoleInfo(dataMap);
    role.setName(roleInfo.getName());
    role.setDescription(roleInfo.getDescription());
  }
}
