/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.role.mappers;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubRole;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.policy.DataHubRoleInfo;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DataHubRoleMapper implements ModelMapper<EntityResponse, DataHubRole> {

  public static final DataHubRoleMapper INSTANCE = new DataHubRoleMapper();

  public static DataHubRole map(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public DataHubRole apply(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
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
