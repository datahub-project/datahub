package com.linkedin.datahub.graphql.types.dataplatform;

import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.types.EntityType;
import com.linkedin.datahub.graphql.types.mappers.DataPlatformInfoMapper;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.dataplatform.client.DataPlatforms;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DataPlatformType implements EntityType<DataPlatform> {

    private final DataPlatforms _dataPlatformsClient;
    private Map<String, DataPlatform> _urnToPlatform;

    public DataPlatformType(final DataPlatforms dataPlatformsClient) {
        _dataPlatformsClient = dataPlatformsClient;
    }

    @Override
    public Class<DataPlatform> objectClass() {
        return DataPlatform.class;
    }

    @Override
    public List<DataPlatform> batchLoad(final List<String> urns, final QueryContext context) {
        try {
            if (_urnToPlatform == null) {
                _urnToPlatform = _dataPlatformsClient.getAllPlatforms().stream()
                        .filter(com.linkedin.dataPlatforms.DataPlatform::hasDataPlatformInfo)
                        .map(platform -> new DataPlatform(
                                new DataPlatformUrn(platform.getName()).toString(),
                                com.linkedin.datahub.graphql.generated.EntityType.DATA_PLATFORM,
                                platform.getName(),
                                DataPlatformInfoMapper.map(platform.getDataPlatformInfo())))
                        .collect(Collectors.toMap(DataPlatform::getUrn, platform -> platform));
            }
            return urns.stream()
                    .map(key -> _urnToPlatform.get(key))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load DataPlatforms", e);
        }
    }

    @Override
    public com.linkedin.datahub.graphql.generated.EntityType type() {
        return com.linkedin.datahub.graphql.generated.EntityType.DATA_PLATFORM;
    }
}
