package com.linkedin.datahub.graphql.types.corpuser;

import com.linkedin.common.urn.CorpuserUrn;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.types.corpuser.mappers.CorpUserSnapshotMapper;
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

public class CorpUserType implements SearchableEntityType<CorpUser> {

    private final EntityClient _entityClient;

    public CorpUserType(final EntityClient entityClient) {
        _entityClient = entityClient;
    }

    @Override
    public Class<CorpUser> objectClass() {
        return CorpUser.class;
    }

    @Override
    public EntityType type() {
        return EntityType.CORP_USER;
    }

    @Override
    public List<DataFetcherResult<CorpUser>> batchLoad(final List<String> urns, final QueryContext context) {
        try {
            final List<CorpuserUrn> corpUserUrns = urns
                    .stream()
                    .map(this::getCorpUserUrn)
                    .collect(Collectors.toList());

            final Map<Urn, Entity> corpUserMap = _entityClient
                    .batchGet(new HashSet<>(corpUserUrns), context.getActor());

            final List<Entity> results = new ArrayList<>();
            for (CorpuserUrn urn : corpUserUrns) {
                results.add(corpUserMap.getOrDefault(urn, null));
            }
            return results.stream()
                    .map(gmsCorpUser -> gmsCorpUser == null ? null
                        : DataFetcherResult.<CorpUser>newResult().data(CorpUserSnapshotMapper.map(gmsCorpUser.getValue().getCorpUserSnapshot())).build())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load Datasets", e);
        }
    }

    @Override
    public SearchResults search(@Nonnull String query,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count,
                                @Nonnull final QueryContext context) throws Exception {
        final SearchResult searchResult = _entityClient.search("corpuser", query, Collections.emptyMap(), start, count,
            context.getActor());
        return UrnSearchResultsMapper.map(searchResult);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit,
                                            @Nonnull final QueryContext context) throws Exception {
        final AutoCompleteResult result = _entityClient.autoComplete("corpuser", query, Collections.emptyMap(), limit, context.getActor());
        return AutoCompleteResultsMapper.map(result);
    }

    private CorpuserUrn getCorpUserUrn(final String urnStr) {
        try {
            return CorpuserUrn.createFromString(urnStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Failed to retrieve user with urn %s, invalid urn", urnStr));
        }
    }
}
