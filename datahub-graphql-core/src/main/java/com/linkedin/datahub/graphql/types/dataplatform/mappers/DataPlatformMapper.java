package com.linkedin.datahub.graphql.types.dataplatform.mappers;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.common.mappers.util.SystemMetadataUtils;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.DataPlatformKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DataPlatformMapper implements ModelMapper<EntityResponse, DataPlatform> {

  public static final DataPlatformMapper INSTANCE = new DataPlatformMapper();

  public static DataPlatform map(
      @Nullable QueryContext context, @Nonnull final EntityResponse platform) {
    return INSTANCE.apply(context, platform);
  }

  @Override
  public DataPlatform apply(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final DataPlatform result = new DataPlatform();
    final DataPlatformKey dataPlatformKey =
        (DataPlatformKey)
            EntityKeyUtils.convertUrnToEntityKeyInternal(
                entityResponse.getUrn(), new DataPlatformKey().schema());
    result.setType(EntityType.DATA_PLATFORM);
    Urn urn = entityResponse.getUrn();
    result.setUrn(urn.toString());
    result.setName(dataPlatformKey.getPlatformName());

    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    Long lastIngested = SystemMetadataUtils.getLastIngestedTime(aspectMap);
    result.setLastIngested(lastIngested);

    MappingHelper<DataPlatform> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(
        DATA_PLATFORM_KEY_ASPECT_NAME,
        (dataPlatform, dataMap) ->
            dataPlatform.setName(new DataPlatformKey(dataMap).getPlatformName()));
    mappingHelper.mapToResult(
        DATA_PLATFORM_INFO_ASPECT_NAME,
        (dataPlatform, dataMap) ->
            dataPlatform.setProperties(
                DataPlatformPropertiesMapper.map(context, new DataPlatformInfo(dataMap))));
    return mappingHelper.getResult();
  }
}
