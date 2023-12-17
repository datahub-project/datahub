package com.linkedin.datahub.graphql.types.businessattribute;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BusinessAttribute;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.businessattribute.mappers.BusinessAttributeMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import graphql.execution.DataFetcherResult;
import lombok.RequiredArgsConstructor;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.linkedin.metadata.Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.BUSINESS_ATTRIBUTE_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;

@RequiredArgsConstructor
public class BusinessAttributeType implements com.linkedin.datahub.graphql.types.EntityType<BusinessAttribute, String> {

    public static final Set<String> ASPECTS_TO_FETCH = ImmutableSet.of(
            BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME,
            BUSINESS_ATTRIBUTE_KEY_ASPECT_NAME,
            OWNERSHIP_ASPECT_NAME,
            INSTITUTIONAL_MEMORY_ASPECT_NAME,
            STATUS_ASPECT_NAME
    );

    private final EntityClient _entityClient;

    @Override
    public EntityType type() {
        return EntityType.BUSINESS_ATTRIBUTE;
    }

    @Override
    public Function<Entity, String> getKeyProvider() {
        return Entity::getUrn;
    }

    @Override
    public Class<BusinessAttribute> objectClass() {
        return BusinessAttribute.class;
    }

    @Override
    public List<DataFetcherResult<BusinessAttribute>> batchLoad(@Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
        final List<Urn> businessAttributeUrns = urns.stream()
                .map(UrnUtils::getUrn)
                .collect(Collectors.toList());

        try {
            final Map<Urn, EntityResponse> businessAttributeMap = _entityClient.batchGetV2(BUSINESS_ATTRIBUTE_ENTITY_NAME,
                    new HashSet<>(businessAttributeUrns), ASPECTS_TO_FETCH, context.getAuthentication());

            final List<EntityResponse> gmsResults = new ArrayList<>();
            for (Urn urn : businessAttributeUrns) {
                gmsResults.add(businessAttributeMap.getOrDefault(urn, null));
            }
            return gmsResults.stream()
                    .map(gmsResult -> gmsResult == null ? null
                            : DataFetcherResult.<BusinessAttribute>newResult()
                            .data(BusinessAttributeMapper.map(gmsResult))
                            .build())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load Business Attributes", e);
        }
    }
}
