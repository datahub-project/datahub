package com.linkedin.datahub.graphql.types.customfabrictype;

import static com.linkedin.metadata.Constants.CUSTOM_FABRIC_TYPE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DEPRECATION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;

import com.linkedin.common.Deprecation;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CustomFabricType;
import com.linkedin.datahub.graphql.generated.CustomFabricTypeProperties;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.fabrictype.CustomFabricTypeInfo;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class CustomFabricTypeMapper implements ModelMapper<EntityResponse, CustomFabricType> {

  public static final CustomFabricTypeMapper INSTANCE = new CustomFabricTypeMapper();

  public static CustomFabricType map(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  /** The fields that come from the URN alone, with no aspect. */
  @Nonnull
  public static CustomFabricType fromUrn(@Nonnull Urn urn) {
    CustomFabricType result = new CustomFabricType();
    result.setUrn(urn.toString());
    result.setType(EntityType.CUSTOM_FABRIC_TYPE);
    result.setId(urn.getId());
    return result;
  }

  @Nonnull
  public static CustomFabricTypeProperties properties(
      @Nullable String displayName, @Nullable String description) {
    CustomFabricTypeProperties properties = new CustomFabricTypeProperties();
    properties.setDisplayName(displayName);
    properties.setDescription(description);
    return properties;
  }

  @Override
  public CustomFabricType apply(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final Urn urn = entityResponse.getUrn();
    final MappingHelper<CustomFabricType> mappingHelper =
        new MappingHelper<>(entityResponse.getAspects(), fromUrn(urn));
    mappingHelper.mapToResult(
        CUSTOM_FABRIC_TYPE_INFO_ASPECT_NAME,
        (type, dataMap) -> {
          CustomFabricTypeInfo info = new CustomFabricTypeInfo(dataMap);
          type.setProperties(properties(info.getDisplayName(), info.getDescription()));
        });
    mappingHelper.mapToResult(
        OWNERSHIP_ASPECT_NAME,
        (type, dataMap) ->
            type.setOwnership(OwnershipMapper.map(context, new Ownership(dataMap), urn)));
    mappingHelper.mapToResult(
        STATUS_ASPECT_NAME,
        (type, dataMap) -> type.setStatus(StatusMapper.map(context, new Status(dataMap))));
    mappingHelper.mapToResult(
        DEPRECATION_ASPECT_NAME,
        (type, dataMap) ->
            type.setDeprecation(DeprecationMapper.map(context, new Deprecation(dataMap))));
    return mappingHelper.getResult();
  }
}
