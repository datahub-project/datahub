package com.linkedin.datahub.graphql.types.chart;

import com.linkedin.common.urn.ChartUrn;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
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
import com.linkedin.datahub.graphql.types.chart.mappers.ChartSnapshotMapper;
import com.linkedin.datahub.graphql.types.chart.mappers.ChartUpdateInputSnapshotMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowseResultMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.configs.ChartSearchConfig;
import com.linkedin.metadata.extractor.AspectExtractor;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SearchResult;
import com.linkedin.metadata.snapshot.ChartSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.r2.RemoteInvocationException;

import graphql.execution.DataFetcherResult;
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

    private static final ChartSearchConfig CHART_SEARCH_CONFIG = new ChartSearchConfig();
    private final EntityClient _entityClient;

    public ChartType(final EntityClient entityClient)  {
        _entityClient = entityClient;
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
    public List<DataFetcherResult<Chart>> batchLoad(@Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
        final List<Urn> chartUrns = urns.stream()
                .map(this::getChartUrn)
                .collect(Collectors.toList());

        try {
            final Map<Urn, com.linkedin.entity.Entity> chartMap = _entityClient.batchGet(chartUrns
                    .stream()
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet()));

            final List<com.linkedin.entity.Entity> gmsResults = new ArrayList<>();
            for (Urn urn : chartUrns) {
                gmsResults.add(chartMap.getOrDefault(urn, null));
            }
            return gmsResults.stream()
                    .map(gmsChart -> gmsChart == null ? null
                        : DataFetcherResult.<Chart>newResult()
                            .data(ChartSnapshotMapper.map(gmsChart.getValue().getChartSnapshot()))
                            .localContext(AspectExtractor.extractAspects(gmsChart.getValue().getChartSnapshot()))
                            .build())
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
        final SearchResult searchResult = _entityClient.search(
            "chart", query, facetFilters, start, count);
        return UrnSearchResultsMapper.map(searchResult);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit,
                                            @Nonnull QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, CHART_SEARCH_CONFIG.getFacetFields());
        final AutoCompleteResult result = _entityClient.autoComplete("chart", query, facetFilters, limit);
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
        final BrowseResult result = _entityClient.browse(
                "chart",
                pathStr,
                facetFilters,
                start,
                count);
        return BrowseResultMapper.map(result);
    }

    @Override
    public List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull QueryContext context) throws Exception {
        final StringArray result = _entityClient.getBrowsePaths(getChartUrn(urn));
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

        final CorpuserUrn actor = CorpuserUrn.createFromString(context.getActor());
        final ChartSnapshot chartSnapshot = ChartUpdateInputSnapshotMapper.map(input, actor);
        final Snapshot snapshot = Snapshot.create(chartSnapshot);

        try {
            _entityClient.update(new com.linkedin.entity.Entity().setValue(snapshot));
        } catch (RemoteInvocationException e) {
            throw new RuntimeException(String.format("Failed to write entity with urn %s", input.getUrn()), e);
        }

        return load(input.getUrn(), context).getData();
    }
}
