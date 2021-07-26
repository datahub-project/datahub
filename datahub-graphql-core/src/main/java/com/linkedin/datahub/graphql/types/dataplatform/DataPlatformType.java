package com.linkedin.datahub.graphql.types.dataplatform;

import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.types.EntityType;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.types.dataplatform.mappers.DataPlatformMapper;
import com.linkedin.dataplatform.client.DataPlatforms;

import graphql.execution.DataFetcherResult;
import java.net.URISyntaxException;
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
    public List<DataFetcherResult<DataPlatform>> batchLoad(final List<String> urns, final QueryContext context) {
        try {
            if (_urnToPlatform == null) {
                _urnToPlatform = _dataPlatformsClient.getAllPlatforms().stream()
                        .map(DataPlatformMapper::map)
                        .collect(Collectors.toMap(DataPlatform::getUrn, platform -> platform));
            }
            return urns.stream()
                    .map(key -> _urnToPlatform.containsKey(key) ? _urnToPlatform.get(key) : getUnknownDataPlatform(key))
                .map(dataPlatform -> DataFetcherResult.<DataPlatform>newResult().data(dataPlatform).build())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load DataPlatforms", e);
        }
    }

    @Override
    public com.linkedin.datahub.graphql.generated.EntityType type() {
        return com.linkedin.datahub.graphql.generated.EntityType.DATA_PLATFORM;
    }

    private DataPlatform getUnknownDataPlatform(final String urnStr) {
        try {
            final com.linkedin.dataPlatforms.DataPlatform platform = new com.linkedin.dataPlatforms.DataPlatform()
                    .setName(DataPlatformUrn.createFromString(urnStr).getPlatformNameEntity());
            return DataPlatformMapper.map(platform);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Invalid DataPlatformUrn %s provided", urnStr), e);
        }
    }
}
