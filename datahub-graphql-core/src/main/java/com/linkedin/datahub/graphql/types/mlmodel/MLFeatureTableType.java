package com.linkedin.datahub.graphql.types.mlmodel;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.MLFeatureTable;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowseResultMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.MLFeatureTableSnapshotMapper;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.extractor.AspectExtractor;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SearchResult;
import graphql.execution.DataFetcherResult;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.linkedin.datahub.graphql.Constants.BROWSE_PATH_DELIMITER;

public class MLFeatureTableType implements SearchableEntityType<MLFeatureTable>, BrowsableEntityType<MLFeatureTable> {

    private static final Set<String> FACET_FIELDS = ImmutableSet.of("platform", "name");
    private final EntityClient _mlFeatureTableClient;

    public MLFeatureTableType(final EntityClient mlFeatureTableClient) {
        _mlFeatureTableClient = mlFeatureTableClient;
    }

    @Override
    public EntityType type() {
        return EntityType.MLFEATURE_TABLE;
    }

    @Override
    public Class<MLFeatureTable> objectClass() {
        return MLFeatureTable.class;
    }

    @Override
    public List<DataFetcherResult<MLFeatureTable>> batchLoad(final List<String> urns, final QueryContext context) throws Exception {
        final List<Urn> mlFeatureTableUrns = urns.stream()
            .map(MLModelUtils::getUrn)
            .collect(Collectors.toList());

        try {
            final Map<Urn, Entity> mlFeatureTableMap = _mlFeatureTableClient.batchGet(mlFeatureTableUrns
                .stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

            final List<Entity> gmsResults = mlFeatureTableUrns.stream()
                .map(featureTableUrn -> mlFeatureTableMap.getOrDefault(featureTableUrn, null)).collect(Collectors.toList());

            return gmsResults.stream()
                .map(gmsMlFeatureTable -> gmsMlFeatureTable == null ? null
                    : DataFetcherResult.<MLFeatureTable>newResult()
                        .data(MLFeatureTableSnapshotMapper.map(gmsMlFeatureTable.getValue().getMLFeatureTableSnapshot()))
                        .localContext(AspectExtractor.extractAspects(gmsMlFeatureTable.getValue().getMLFeatureTableSnapshot()))
                        .build())
                .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load MLFeatureTables", e);
        }
    }

    @Override
    public SearchResults search(@Nonnull String query,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count,
                                @Nonnull final QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final SearchResult searchResult = _mlFeatureTableClient.search("mlFeatureTable", query, facetFilters, start, count);
        return UrnSearchResultsMapper.map(searchResult);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit,
                                            @Nonnull final QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final AutoCompleteResult result = _mlFeatureTableClient.autoComplete("mlFeatureTable", query, facetFilters, limit);
        return AutoCompleteResultsMapper.map(result);
    }

    @Override
    public BrowseResults browse(@Nonnull List<String> path,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count,
                                @Nonnull final QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final String pathStr = path.size() > 0 ? BROWSE_PATH_DELIMITER + String.join(BROWSE_PATH_DELIMITER, path) : "";
        final BrowseResult result = _mlFeatureTableClient.browse(
                "mlFeatureTable",
                pathStr,
                facetFilters,
                start,
                count);
        return BrowseResultMapper.map(result);
    }

    @Override
    public List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull final QueryContext context) throws Exception {
        final StringArray result = _mlFeatureTableClient.getBrowsePaths(MLModelUtils.getUrn(urn));
        return BrowsePathsMapper.map(result);
    }
}
