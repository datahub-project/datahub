package com.linkedin.datahub.graphql.types.corpgroup;

import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.types.corpgroup.mappers.CorpGroupSnapshotMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.search.SearchResult;

import graphql.execution.DataFetcherResult;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CorpGroupType implements SearchableEntityType<CorpGroup> {

    private final EntityClient _entityClient;

    public CorpGroupType(final EntityClient entityClient) {
        _entityClient = entityClient;
    }

    @Override
    public Class<CorpGroup> objectClass() {
        return CorpGroup.class;
    }

    @Override
    public EntityType type() {
        return EntityType.CORP_GROUP;
    }

    @Override
    public List<DataFetcherResult<CorpGroup>> batchLoad(final List<String> urns, final QueryContext context) {
        try {
            final List<CorpGroupUrn> corpGroupUrns = urns
                    .stream()
                    .map(this::getCorpGroupUrn)
                    .collect(Collectors.toList());

            final Map<Urn, Entity> corpGroupMap = _entityClient
                    .batchGet(new HashSet<>(corpGroupUrns), context.getActor());

            final List<Entity> results = new ArrayList<>();
            for (CorpGroupUrn urn : corpGroupUrns) {
                results.add(corpGroupMap.getOrDefault(urn, null));
            }
            return results.stream()
                    .map(gmsCorpGroup -> gmsCorpGroup == null ? null
                        : DataFetcherResult.<CorpGroup>newResult().data(CorpGroupSnapshotMapper.map(gmsCorpGroup.getValue().getCorpGroupSnapshot())).build())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load CorpGroup", e);
        }
    }

    @Override
    public SearchResults search(@Nonnull String query,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count,
                                @Nonnull final QueryContext context) throws Exception {
        final SearchResult
            searchResult = _entityClient.search("corpGroup", query, Collections.emptyMap(), start, count,
            context.getActor());
        return UrnSearchResultsMapper.map(searchResult);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit,
                                            @Nonnull final QueryContext context) throws Exception {
        final AutoCompleteResult result = _entityClient.autoComplete("corpGroup", query, Collections.emptyMap(), limit,
            context.getActor());
        return AutoCompleteResultsMapper.map(result);
    }

    private CorpGroupUrn getCorpGroupUrn(final String urnStr) {
        try {
            return CorpGroupUrn.createFromString(urnStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Failed to retrieve CorpGroup with urn %s, invalid urn", urnStr));
        }
    }
}
