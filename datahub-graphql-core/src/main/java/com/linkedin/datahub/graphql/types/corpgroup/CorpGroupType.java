package com.linkedin.datahub.graphql.types.corpgroup;

import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.corpgroup.mappers.CorpGroupMapper;
import com.linkedin.datahub.graphql.types.mappers.SearchResultsMapper;
import com.linkedin.identity.client.CorpGroups;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.restli.common.CollectionResponse;

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

    private static final String DEFAULT_AUTO_COMPLETE_FIELD = "name";

    private final CorpGroups _corpGroupsClient;

    public CorpGroupType(final CorpGroups corpGroupsClient) {
        _corpGroupsClient = corpGroupsClient;
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
    public List<CorpGroup> batchLoad(final List<String> urns, final QueryContext context) {
        try {
            final List<CorpGroupUrn> corpGroupUrns = urns
                    .stream()
                    .map(this::getCorpGroupUrn)
                    .collect(Collectors.toList());

            final Map<CorpGroupUrn, com.linkedin.identity.CorpGroup> corpGroupMap = _corpGroupsClient
                    .batchGet(new HashSet<>(corpGroupUrns));

            final List<com.linkedin.identity.CorpGroup> results = new ArrayList<>();
            for (CorpGroupUrn urn : corpGroupUrns) {
                results.add(corpGroupMap.getOrDefault(urn, null));
            }
            return results.stream()
                    .map(gmsCorpGroup -> gmsCorpGroup == null ? null : CorpGroupMapper.map(gmsCorpGroup))
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
        final CollectionResponse<com.linkedin.identity.CorpGroup> searchResult = _corpGroupsClient.search(query, Collections.emptyMap(), start, count);
        return SearchResultsMapper.map(searchResult, CorpGroupMapper::map);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit,
                                            @Nonnull final QueryContext context) throws Exception {
        field = field != null ? field : DEFAULT_AUTO_COMPLETE_FIELD;
        final AutoCompleteResult result = _corpGroupsClient.autocomplete(query, field, Collections.emptyMap(), limit);
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
