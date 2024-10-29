package com.linkedin.datahub.graphql.types.dataplatforminstance.mappers;

import com.linkedin.common.Deprecation;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.DataPlatformInstance;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.CustomPropertiesMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.dataplatforminstance.DataPlatformInstanceProperties;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DataPlatformInstanceKey;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DataPlatformInstanceMapper {

  public static final DataPlatformInstanceMapper INSTANCE = new DataPlatformInstanceMapper();

  public static DataPlatformInstance map(
      @Nullable QueryContext context, final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  public DataPlatformInstance apply(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final DataPlatformInstance result = new DataPlatformInstance();
    final Urn entityUrn = entityResponse.getUrn();
    result.setUrn(entityUrn.toString());
    result.setType(EntityType.DATA_PLATFORM_INSTANCE);

    final EnvelopedAspectMap aspects = entityResponse.getAspects();
    MappingHelper<DataPlatformInstance> mappingHelper = new MappingHelper<>(aspects, result);
    mappingHelper.mapToResult(
        Constants.DATA_PLATFORM_INSTANCE_KEY_ASPECT_NAME, this::mapDataPlatformInstanceKey);
    mappingHelper.mapToResult(
        Constants.DATA_PLATFORM_INSTANCE_PROPERTIES_ASPECT_NAME,
        (dataPlatformInstance, dataMap) ->
            this.mapDataPlatformInstanceProperties(dataPlatformInstance, dataMap, entityUrn));
    mappingHelper.mapToResult(
        Constants.OWNERSHIP_ASPECT_NAME,
        (dataPlatformInstance, dataMap) ->
            dataPlatformInstance.setOwnership(
                OwnershipMapper.map(context, new Ownership(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        Constants.GLOBAL_TAGS_ASPECT_NAME,
        (dataPlatformInstance, dataMap) ->
            this.mapGlobalTags(context, dataPlatformInstance, dataMap, entityUrn));
    mappingHelper.mapToResult(
        Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME,
        (dataPlatformInstance, dataMap) ->
            dataPlatformInstance.setInstitutionalMemory(
                InstitutionalMemoryMapper.map(
                    context, new InstitutionalMemory(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        Constants.STATUS_ASPECT_NAME,
        (dataPlatformInstance, dataMap) ->
            dataPlatformInstance.setStatus(StatusMapper.map(context, new Status(dataMap))));
    mappingHelper.mapToResult(
        Constants.DEPRECATION_ASPECT_NAME,
        (dataPlatformInstance, dataMap) ->
            dataPlatformInstance.setDeprecation(
                DeprecationMapper.map(context, new Deprecation(dataMap))));
    return mappingHelper.getResult();
  }

  private void mapDataPlatformInstanceKey(
      @Nonnull DataPlatformInstance dataPlatformInstance, @Nonnull DataMap dataMap) {
    final DataPlatformInstanceKey gmsKey = new DataPlatformInstanceKey(dataMap);
    dataPlatformInstance.setPlatform(
        DataPlatform.builder()
            .setType(EntityType.DATA_PLATFORM)
            .setUrn(gmsKey.getPlatform().toString())
            .build());
    dataPlatformInstance.setInstanceId(gmsKey.getInstance());
  }

  private void mapDataPlatformInstanceProperties(
      @Nonnull DataPlatformInstance dataPlatformInstance,
      @Nonnull DataMap dataMap,
      @Nonnull Urn entityUrn) {
    final DataPlatformInstanceProperties gmsProperties =
        new DataPlatformInstanceProperties(dataMap);
    final com.linkedin.datahub.graphql.generated.DataPlatformInstanceProperties properties =
        new com.linkedin.datahub.graphql.generated.DataPlatformInstanceProperties();
    properties.setName(gmsProperties.getName());
    properties.setDescription(gmsProperties.getDescription());
    if (gmsProperties.hasExternalUrl()) {
      properties.setExternalUrl(gmsProperties.getExternalUrl().toString());
    }
    if (gmsProperties.hasCustomProperties()) {
      properties.setCustomProperties(
          CustomPropertiesMapper.map(gmsProperties.getCustomProperties(), entityUrn));
    }

    dataPlatformInstance.setProperties(properties);
  }

  private static void mapGlobalTags(
      @Nullable QueryContext context,
      @Nonnull DataPlatformInstance dataPlatformInstance,
      @Nonnull DataMap dataMap,
      @Nonnull final Urn entityUrn) {
    com.linkedin.datahub.graphql.generated.GlobalTags globalTags =
        GlobalTagsMapper.map(context, new GlobalTags(dataMap), entityUrn);
    dataPlatformInstance.setTags(globalTags);
  }
}
