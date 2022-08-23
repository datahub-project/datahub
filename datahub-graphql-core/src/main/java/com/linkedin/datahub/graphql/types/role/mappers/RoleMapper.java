package com.linkedin.datahub.graphql.types.role.mappers;

import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Role;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.policy.DataHubRoleInfo;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


public class RoleMapper implements ModelMapper<EntityResponse, Role> {

  public static final com.linkedin.datahub.graphql.types.role.mappers.RoleMapper INSTANCE =
      new com.linkedin.datahub.graphql.types.role.mappers.RoleMapper();

  public static Role map(@Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(entityResponse);
  }

  @Override
  public Role apply(@Nonnull final EntityResponse entityResponse) {
    final Role result = new Role();

    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.ROLE);
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<Role> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(DATAHUB_ROLE_INFO_ASPECT_NAME, this::mapDataHubRoleInfo);
    return mappingHelper.getResult();
  }

  private void mapDataHubRoleInfo(@Nonnull Role role, @Nonnull DataMap dataMap) {
    DataHubRoleInfo roleInfo = new DataHubRoleInfo(dataMap);
    role.setName(roleInfo.getName());
    role.setDescription(roleInfo.getDescription());
  }
}
