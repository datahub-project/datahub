package com.linkedin.datahub.graphql.types.dataplatforminstance;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataPlatformInstance;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.dataplatforminstance.mappers.DataPlatformInstanceMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.execution.DataFetcherResult;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DataPlatformInstanceType implements com.linkedin.datahub.graphql.types.EntityType<DataPlatformInstance, String> {

    static final Set<String> ASPECTS_TO_FETCH = ImmutableSet.of();
    private final EntityClient _entityClient;

    public DataPlatformInstanceType(final EntityClient entityClient) {
        _entityClient = entityClient;
    }

    @Override
    public EntityType type() {
        return EntityType.DATA_PLATFORM_INSTANCE;
    }

    @Override
    public Function<Entity, String> getKeyProvider() {
        return Entity::getUrn;
    }

    @Override
    public Class<DataPlatformInstance> objectClass() {
        return DataPlatformInstance.class;
    }

    @Override
    public List<DataFetcherResult<DataPlatformInstance>> batchLoad(@Nonnull List<String> urns, @Nonnull QueryContext context) throws  Exception {
        final List<Urn> dataPlatformInstanceUrns = urns.stream()
                .map(UrnUtils::getUrn)
                .collect(Collectors.toList());

        try {
            final Map<Urn, EntityResponse> entities = _entityClient.batchGetV2(
                    Constants.DATA_PLATFORM_INSTANCE_ENTITY_NAME,
                    new HashSet<>(dataPlatformInstanceUrns),
                    ASPECTS_TO_FETCH,
                    context.getAuthentication());

            final List<EntityResponse> gmsResults = new ArrayList<>();
            for (Urn urn : dataPlatformInstanceUrns) {
                gmsResults.add(entities.getOrDefault(urn, null));
            }
            return gmsResults.stream()
                    .map(gmsResult ->
                            gmsResult == null ? null : DataFetcherResult.<DataPlatformInstance>newResult()
                                    .data(DataPlatformInstanceMapper.map(gmsResult))
                                    .build()
                    )
                    .collect(Collectors.toList());

        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load DataPlatformInstance");
        }
    }

}
