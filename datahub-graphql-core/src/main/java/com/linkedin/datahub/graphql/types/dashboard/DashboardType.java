package com.linkedin.datahub.graphql.types.dashboard;

import com.linkedin.common.urn.DashboardUrn;
import com.linkedin.dashboard.client.Dashboards;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.Dashboard;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.DashboardMapper;
import com.linkedin.datahub.graphql.types.mappers.SearchResultsMapper;
import com.linkedin.metadata.configs.DashboardSearchConfig;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.restli.common.CollectionResponse;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class DashboardType implements SearchableEntityType<Dashboard> {

    private final Dashboards _dashboardsClient;
    private static final DashboardSearchConfig DASHBOARDS_SEARCH_CONFIG = new DashboardSearchConfig();

    public DashboardType(final Dashboards dashboardsClient) {
        _dashboardsClient = dashboardsClient;
    }

    @Override
    public EntityType type() {
        return EntityType.DASHBOARD;
    }

    @Override
    public Class<Dashboard> objectClass() {
        return Dashboard.class;
    }

    @Override
    public List<Dashboard> batchLoad(@Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
        final List<DashboardUrn> dashboardUrns = urns.stream()
                .map(this::getDashboardUrn)
                .collect(Collectors.toList());

        try {
            final Map<DashboardUrn, com.linkedin.dashboard.Dashboard> dashboardMap = _dashboardsClient.batchGet(dashboardUrns
                    .stream()
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet()));

            final List<com.linkedin.dashboard.Dashboard> gmsResults = new ArrayList<>();
            for (DashboardUrn urn : dashboardUrns) {
                gmsResults.add(dashboardMap.getOrDefault(urn, null));
            }
            return gmsResults.stream()
                    .map(gmsDataset -> gmsDataset == null ? null : DashboardMapper.map(gmsDataset))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load Dashboards", e);
        }
    }

    @Override
    public SearchResults search(@Nonnull String query,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count,
                                @Nonnull QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, DASHBOARDS_SEARCH_CONFIG.getFacetFields());
        final CollectionResponse<com.linkedin.dashboard.Dashboard> searchResult = _dashboardsClient.search(query, null, facetFilters, null, start, count);
        return SearchResultsMapper.map(searchResult, DashboardMapper::map);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit,
                                            @Nonnull QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, DASHBOARDS_SEARCH_CONFIG.getFacetFields());
        final AutoCompleteResult result = _dashboardsClient.autocomplete(query, field, facetFilters, limit);
        return AutoCompleteResultsMapper.map(result);
    }

    private com.linkedin.common.urn.DashboardUrn getDashboardUrn(String urnStr) {
        try {
            return DashboardUrn.createFromString(urnStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Failed to retrieve dashboard with urn %s, invalid urn", urnStr));
        }
    }

}
