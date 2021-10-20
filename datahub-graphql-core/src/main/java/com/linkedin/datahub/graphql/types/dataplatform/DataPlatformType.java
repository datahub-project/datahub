package com.linkedin.datahub.graphql.types.dataplatform;

import com.linkedin.common.urn.Urn;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.types.EntityType;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.types.dataplatform.mappers.DataPlatformSnapshotMapper;

import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.extractor.AspectExtractor;
import graphql.execution.DataFetcherResult;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class DataPlatformType implements EntityType<DataPlatform> {

    private final EntityClient _entityClient;

    public DataPlatformType(final EntityClient entityClient) {
        _entityClient = entityClient;
    }

    @Override
    public Class<DataPlatform> objectClass() {
        return DataPlatform.class;
    }

    @Override
    public List<DataFetcherResult<DataPlatform>> batchLoad(final List<String> urns, final QueryContext context) {

        final List<Urn> dataPlatformUrns = urns.stream()
            .map(urnStr -> {
                try {
                    return Urn.createFromString(urnStr);
                } catch (URISyntaxException e) {
                    throw new RuntimeException(String.format("Failed to retrieve entity with urn %s", urnStr));
                }
            })
            .collect(Collectors.toList());

        try {
            final Map<Urn, com.linkedin.entity.Entity> dataPlatformMap = _entityClient.batchGet(dataPlatformUrns
                .stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()),
            context.getActor());

            final List<com.linkedin.entity.Entity> gmsResults = new ArrayList<>();
            for (Urn urn : dataPlatformUrns) {
                gmsResults.add(dataPlatformMap.getOrDefault(urn, null));
            }

            return gmsResults.stream()
                .map(gmsPlatform -> gmsPlatform == null ? null
                    : DataFetcherResult.<DataPlatform>newResult()
                        .data(DataPlatformSnapshotMapper.map(gmsPlatform.getValue().getDataPlatformSnapshot()))
                        .localContext(AspectExtractor.extractAspects(
                            gmsPlatform.getValue().getDataPlatformSnapshot()))
                        .build())
                .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load Data Platforms", e);
        }
    }

    @Override
    public com.linkedin.datahub.graphql.generated.EntityType type() {
        return com.linkedin.datahub.graphql.generated.EntityType.DATA_PLATFORM;
    }
}
