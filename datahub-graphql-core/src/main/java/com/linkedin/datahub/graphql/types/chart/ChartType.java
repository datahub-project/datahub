package com.linkedin.datahub.graphql.types.chart;

import com.linkedin.chart.client.Charts;
import com.linkedin.common.urn.ChartUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.datahub.graphql.generated.ChartUpdateInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.MutableType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.chart.mappers.ChartUpdateInputMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowseResultMetadataMapper;
import com.linkedin.datahub.graphql.types.chart.mappers.ChartMapper;
import com.linkedin.datahub.graphql.types.mappers.SearchResultsMapper;
import com.linkedin.metadata.configs.ChartSearchConfig;
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

public class ChartType implements SearchableEntityType<Chart>, BrowsableEntityType<Chart>, MutableType<ChartUpdateInput> {

    private final Charts _chartsClient;
    private static final ChartSearchConfig CHART_SEARCH_CONFIG = new ChartSearchConfig();

    public ChartType(final Charts chartsClient) {
        _chartsClient = chartsClient;
    }

    @Override
    public Class<ChartUpdateInput> inputClass() {
        return ChartUpdateInput.class;
    }

    @Override
    public EntityType type() {
        return EntityType.CHART;
    }

    @Override
    public Class<Chart> objectClass() {
        return Chart.class;
    }

    @Override
    public List<Chart> batchLoad(@Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
        final List<ChartUrn> chartUrns = urns.stream()
                .map(this::getChartUrn)
                .collect(Collectors.toList());

        try {
            final Map<ChartUrn, com.linkedin.dashboard.Chart> chartMap = _chartsClient.batchGet(chartUrns
                    .stream()
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet()));

            final List<com.linkedin.dashboard.Chart> gmsResults = new ArrayList<>();
            for (ChartUrn urn : chartUrns) {
                gmsResults.add(chartMap.getOrDefault(urn, null));
            }
            return gmsResults.stream()
                    .map(gmsChart -> gmsChart == null ? null : ChartMapper.map(gmsChart))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load Charts", e);
        }
    }

    @Override
    public SearchResults search(@Nonnull String query,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count,
                                @Nonnull QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, CHART_SEARCH_CONFIG.getFacetFields());
        final CollectionResponse<com.linkedin.dashboard.Chart> searchResult = _chartsClient.search(query, null, facetFilters, null, start, count);
        return SearchResultsMapper.map(searchResult, ChartMapper::map);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit,
                                            @Nonnull QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, CHART_SEARCH_CONFIG.getFacetFields());
        final AutoCompleteResult result = _chartsClient.autocomplete(query, field, facetFilters, limit);
        return AutoCompleteResultsMapper.map(result);
    }

    @Override
    public BrowseResults browse(@Nonnull List<String> path,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count,
                                @Nonnull QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, CHART_SEARCH_CONFIG.getFacetFields());
        final String pathStr = path.size() > 0 ? BROWSE_PATH_DELIMITER + String.join(BROWSE_PATH_DELIMITER, path) : "";
        final BrowseResult result = _chartsClient.browse(
                pathStr,
                facetFilters,
                start,
                count);
        final List<String> urns = result.getEntities().stream().map(entity -> entity.getUrn().toString()).collect(Collectors.toList());
        final List<Chart> charts = batchLoad(urns, context);
        final BrowseResults browseResults = new BrowseResults();
        browseResults.setStart(result.getFrom());
        browseResults.setCount(result.getPageSize());
        browseResults.setTotal(result.getNumEntities());
        browseResults.setMetadata(BrowseResultMetadataMapper.map(result.getMetadata()));
        browseResults.setEntities(charts.stream()
                .map(chart -> (com.linkedin.datahub.graphql.generated.Entity) chart)
                .collect(Collectors.toList()));
        return browseResults;
    }

    @Override
    public List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull QueryContext context) throws Exception {
        final StringArray result = _chartsClient.getBrowsePaths(getChartUrn(urn));
        return BrowsePathsMapper.map(result);
    }

    private ChartUrn getChartUrn(String urnStr) {
        try {
            return ChartUrn.createFromString(urnStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Failed to retrieve chart with urn %s, invalid urn", urnStr));
        }
    }

    @Override
    public Chart update(@Nonnull ChartUpdateInput input, @Nonnull QueryContext context) throws Exception {
        final com.linkedin.dashboard.Chart partialChart = ChartUpdateInputMapper.map(input);

        try {
            _chartsClient.update(ChartUrn.createFromString(input.getUrn()), partialChart);
        } catch (RemoteInvocationException e) {
            throw new RuntimeException(String.format("Failed to write entity with urn %s", input.getUrn()), e);
        }

        return load(input.getUrn(), context);
    }
}
