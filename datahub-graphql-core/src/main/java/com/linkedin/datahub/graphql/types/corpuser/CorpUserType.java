package com.linkedin.datahub.graphql.types.corpuser;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.corpuser.mappers.CorpUserMapper;
import com.linkedin.datahub.graphql.types.mappers.SearchResultsMapper;
import com.linkedin.identity.client.CorpUsers;
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

public class CorpUserType implements SearchableEntityType<CorpUser> {

    private static final String DEFAULT_AUTO_COMPLETE_FIELD = "ldap";

    private final CorpUsers _corpUsersClient;

    public CorpUserType(final CorpUsers corpUsersClient) {
        _corpUsersClient = corpUsersClient;
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
    public List<CorpUser> batchLoad(final List<String> urns, final QueryContext context) {
        try {
            final List<CorpuserUrn> corpUserUrns = urns
                    .stream()
                    .map(this::getCorpUserUrn)
                    .collect(Collectors.toList());

            final Map<CorpuserUrn, com.linkedin.identity.CorpUser> corpUserMap = _corpUsersClient
                    .batchGet(new HashSet<>(corpUserUrns));

            final List<com.linkedin.identity.CorpUser> results = new ArrayList<>();
            for (CorpuserUrn urn : corpUserUrns) {
                results.add(corpUserMap.getOrDefault(urn, null));
            }
            return results.stream()
                    .map(gmsCorpUser -> gmsCorpUser == null ? null : CorpUserMapper.map(gmsCorpUser))
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
        final CollectionResponse<com.linkedin.identity.CorpUser> searchResult = _corpUsersClient.search(query, Collections.emptyMap(), start, count);
        return SearchResultsMapper.map(searchResult, CorpUserMapper::map);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit,
                                            @Nonnull final QueryContext context) throws Exception {
        field = field != null ? field : DEFAULT_AUTO_COMPLETE_FIELD;
        final AutoCompleteResult result = _corpUsersClient.autocomplete(query, field, Collections.emptyMap(), limit);
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
