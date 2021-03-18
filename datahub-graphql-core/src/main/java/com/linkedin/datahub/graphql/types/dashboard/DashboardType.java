package com.linkedin.datahub.graphql.types.dashboard;

import com.linkedin.common.urn.DashboardUrn;
import com.linkedin.dashboard.client.Dashboards;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.Dashboard;
import com.linkedin.datahub.graphql.generated.DashboardUpdateInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.MutableType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.dashboard.mappers.DashboardUpdateInputMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowseResultMetadataMapper;
import com.linkedin.datahub.graphql.types.dashboard.mappers.DashboardMapper;
import com.linkedin.datahub.graphql.types.mappers.SearchResultsMapper;
import com.linkedin.metadata.configs.DashboardSearchConfig;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.common.CollectionResponse;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.linkedin.datahub.graphql.Constants.BROWSE_PATH_DELIMITER;

public class DashboardType implements SearchableEntityType<Dashboard>, BrowsableEntityType<Dashboard>, MutableType<DashboardUpdateInput> {

    private final Dashboards _dashboardsClient;
    private static final DashboardSearchConfig DASHBOARDS_SEARCH_CONFIG = new DashboardSearchConfig();

    public DashboardType(final Dashboards dashboardsClient) {
        _dashboardsClient = dashboardsClient;
    }

    @Override
    public Class<DashboardUpdateInput> inputClass() {
        return DashboardUpdateInput.class;
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
                    .map(gmsDashboard -> gmsDashboard == null ? null : DashboardMapper.map(gmsDashboard))
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

    @Override
    public BrowseResults browse(@Nonnull List<String> path,
                                @Nullable List<FacetFilterInput> filters,
                                int start, int count,
                                @Nonnull QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, DASHBOARDS_SEARCH_CONFIG.getFacetFields());
        final String pathStr = path.size() > 0 ? BROWSE_PATH_DELIMITER + String.join(BROWSE_PATH_DELIMITER, path) : "";
        final BrowseResult result = _dashboardsClient.browse(
                pathStr,
                facetFilters,
                start,
                count);
        final List<String> urns = result.getEntities().stream().map(entity -> entity.getUrn().toString()).collect(Collectors.toList());
        final List<Dashboard> dashboards = batchLoad(urns, context);
        final BrowseResults browseResults = new BrowseResults();
        browseResults.setStart(result.getFrom());
        browseResults.setCount(result.getPageSize());
        browseResults.setTotal(result.getNumEntities());
        browseResults.setMetadata(BrowseResultMetadataMapper.map(result.getMetadata()));
        browseResults.setEntities(dashboards.stream()
                .map(dashboard -> (com.linkedin.datahub.graphql.generated.Entity) dashboard)
                .collect(Collectors.toList()));
        return browseResults;
    }

    @Override
    public List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull QueryContext context) throws Exception {
        final StringArray result = _dashboardsClient.getBrowsePaths(getDashboardUrn(urn));
        return BrowsePathsMapper.map(result);
    }

    private com.linkedin.common.urn.DashboardUrn getDashboardUrn(String urnStr) {
        try {
            return DashboardUrn.createFromString(urnStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Failed to retrieve dashboard with urn %s, invalid urn", urnStr));
        }
    }

    @Override
    public Dashboard update(@Nonnull DashboardUpdateInput input, @Nonnull QueryContext context) throws Exception {
        final com.linkedin.dashboard.Dashboard partialDashboard = DashboardUpdateInputMapper.map(input);

        try {
            _dashboardsClient.update(DashboardUrn.createFromString(input.getUrn()), partialDashboard);
        } catch (RemoteInvocationException e) {
            throw new RuntimeException(String.format("Failed to write entity with urn %s", input.getUrn()), e);
        }

        return load(input.getUrn(), context);
    }
}
