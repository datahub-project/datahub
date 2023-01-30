package com.linkedin.datahub.graphql.types.dataplatforminstance.mappers;

import com.linkedin.common.Ownership;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Status;
import com.linkedin.common.Deprecation;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.CustomPropertiesMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.dataplatforminstance.DataPlatformInstanceProperties;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.DataPlatformInstance;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DataPlatformInstanceKey;

import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.DATA_PLATFORM_INSTANCE_KEY_ASPECT_NAME;

public class DataPlatformInstanceMapper {

  public static final DataPlatformInstanceMapper INSTANCE = new DataPlatformInstanceMapper();

  public static DataPlatformInstance map(final EntityResponse entityResponse) {
    return INSTANCE.apply(entityResponse);
  }

  public DataPlatformInstance apply(@Nonnull final EntityResponse entityResponse) {
    final DataPlatformInstance result = new DataPlatformInstance();
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    result.setUrn(entityUrn.toString());
    result.setType(EntityType.DATA_PLATFORM_INSTANCE);

    MappingHelper<DataPlatformInstance> mappingHelper = new MappingHelper<>(aspects, result);
    mappingHelper.mapToResult(DATA_PLATFORM_INSTANCE_KEY_ASPECT_NAME, this::mapDataPlatformInstanceKey);

    final EnvelopedAspect envelopedDataPlatformInstanceProperties = aspects.get(Constants.DATA_PLATFORM_INSTANCE_PROPERTIES_ASPECT_NAME);
    if (envelopedDataPlatformInstanceProperties != null) {
      result.setProperties(mapDataPlatformInstanceProperties(
              new DataPlatformInstanceProperties(envelopedDataPlatformInstanceProperties.getValue().data()), entityUrn)
      );
    }

    final EnvelopedAspect envelopedOwnership = aspects.get(Constants.OWNERSHIP_ASPECT_NAME);
    if (envelopedOwnership != null) {
      result.setOwnership(OwnershipMapper.map(new Ownership(envelopedOwnership.getValue().data()), entityUrn));
    }

    final EnvelopedAspect envelopedTags = aspects.get(Constants.GLOBAL_TAGS_ASPECT_NAME);
    if (envelopedTags != null) {
      com.linkedin.datahub.graphql.generated.GlobalTags globalTags = GlobalTagsMapper.map(new GlobalTags(envelopedTags.getValue().data()), entityUrn);
      result.setTags(globalTags);
    }

    final EnvelopedAspect envelopedInstitutionalMemory = aspects.get(Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME);
    if (envelopedInstitutionalMemory != null) {
      result.setInstitutionalMemory(InstitutionalMemoryMapper.map(new InstitutionalMemory(envelopedInstitutionalMemory.getValue().data())));
    }

    final EnvelopedAspect statusAspect = aspects.get(Constants.STATUS_ASPECT_NAME);
    if (statusAspect != null) {
      result.setStatus(StatusMapper.map(new Status(statusAspect.getValue().data())));
    }

    final EnvelopedAspect envelopedDeprecation = aspects.get(Constants.DEPRECATION_ASPECT_NAME);
    if (envelopedDeprecation != null) {
      result.setDeprecation(DeprecationMapper.map(new Deprecation(envelopedDeprecation.getValue().data())));
    }

    return mappingHelper.getResult();
  }

  private void mapDataPlatformInstanceKey(@Nonnull DataPlatformInstance dataPlatformInstance, @Nonnull DataMap dataMap) {
    final DataPlatformInstanceKey gmsKey = new DataPlatformInstanceKey(dataMap);
    dataPlatformInstance.setPlatform(DataPlatform.builder()
        .setType(EntityType.DATA_PLATFORM)
        .setUrn(gmsKey.getPlatform().toString())
        .build());
    dataPlatformInstance.setInstanceId(gmsKey.getInstance());
  }

  private com.linkedin.datahub.graphql.generated.DataPlatformInstanceProperties mapDataPlatformInstanceProperties(
          final DataPlatformInstanceProperties gmsProperties, Urn entityUrn
  ) {
    final com.linkedin.datahub.graphql.generated.DataPlatformInstanceProperties propertiesResult =
            new com.linkedin.datahub.graphql.generated.DataPlatformInstanceProperties();
    propertiesResult.setName(gmsProperties.getName());
    propertiesResult.setDescription(gmsProperties.getDescription());
    if (gmsProperties.hasExternalUrl()) {
      propertiesResult.setExternalUrl(gmsProperties.getExternalUrl().toString());
    }
    if (gmsProperties.hasCustomProperties()) {
      propertiesResult.setCustomProperties(CustomPropertiesMapper.map(gmsProperties.getCustomProperties(), entityUrn));
    }

    return propertiesResult;
  }

}
