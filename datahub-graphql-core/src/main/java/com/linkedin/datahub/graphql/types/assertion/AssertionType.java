package com.linkedin.datahub.graphql.types.assertion;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.execution.DataFetcherResult;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class AssertionType implements com.linkedin.datahub.graphql.types.EntityType<Assertion, String> {

    static final Set<String> ASPECTS_TO_FETCH = ImmutableSet.of(
        Constants.ASSERTION_KEY_ASPECT_NAME,
        Constants.ASSERTION_INFO_ASPECT_NAME,
        Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME
    );
    private final EntityClient _entityClient;

    public AssertionType(final EntityClient entityClient)  {
        _entityClient = entityClient;
    }

    @Override
    public EntityType type() {
        return EntityType.ASSERTION;
    }

    @Override
    public Function<Entity, String> getKeyProvider() {
        return Entity::getUrn;
    }

    @Override
    public Class<Assertion> objectClass() {
        return Assertion.class;
    }

    @Override
    public List<DataFetcherResult<Assertion>> batchLoad(@Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
        final List<Urn> assertionUrns = urns.stream()
            .map(this::getUrn)
            .collect(Collectors.toList());

        try {
            final Map<Urn, EntityResponse> entities = _entityClient.batchGetV2(
                Constants.ASSERTION_ENTITY_NAME,
                new HashSet<>(assertionUrns),
                ASPECTS_TO_FETCH,
                context.getAuthentication());

            final List<EntityResponse> gmsResults = new ArrayList<>();
            for (Urn urn : assertionUrns) {
                gmsResults.add(entities.getOrDefault(urn, null));
            }
            return gmsResults.stream()
                .map(gmsResult ->
                    gmsResult == null ? null : DataFetcherResult.<Assertion>newResult()
                        .data(AssertionMapper.map(gmsResult))
                        .build()
                )
                .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load Assertions", e);
        }
    }

    private Urn getUrn(final String urnStr) {
        try {
            return Urn.createFromString(urnStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Failed to convert urn string %s into Urn", urnStr));
        }
    }
}