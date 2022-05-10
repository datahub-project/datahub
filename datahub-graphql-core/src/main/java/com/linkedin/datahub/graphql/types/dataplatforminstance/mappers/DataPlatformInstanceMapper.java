package com.linkedin.datahub.graphql.types.dataplatforminstance.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.DataPlatformInstance;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
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
}
