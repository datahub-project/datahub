package com.linkedin.datahub.graphql.types.mlmodel;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.MLFeatureTable;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowseResultMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.MLFeatureTableMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.search.SearchResult;
import graphql.execution.DataFetcherResult;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.datahub.graphql.Constants.*;
import static com.linkedin.metadata.Constants.*;


public class MLFeatureTableType implements SearchableEntityType<MLFeatureTable, String>,
                                           BrowsableEntityType<MLFeatureTable, String> {

    private static final Set<String> FACET_FIELDS = ImmutableSet.of("platform", "name");
    private final EntityClient _entityClient;

    public MLFeatureTableType(final EntityClient entityClient) {
        _entityClient = entityClient;
    }

    @Override
    public EntityType type() {
        return EntityType.MLFEATURE_TABLE;
    }

    @Override
    public Function<Entity, String> getKeyProvider() {
        return Entity::getUrn;
    }

    @Override
    public Class<MLFeatureTable> objectClass() {
        return MLFeatureTable.class;
    }

    @Override
    public List<DataFetcherResult<MLFeatureTable>> batchLoad(final List<String> urns, final QueryContext context) throws Exception {
        final List<Urn> mlFeatureTableUrns = urns.stream()
            .map(UrnUtils::getUrn)
            .collect(Collectors.toList());

        try {
            final Map<Urn, EntityResponse> mlFeatureTableMap = _entityClient.batchGetV2(ML_FEATURE_TABLE_ENTITY_NAME,
                new HashSet<>(mlFeatureTableUrns), null, context.getAuthentication());

            final List<EntityResponse> gmsResults = mlFeatureTableUrns.stream()
                .map(featureTableUrn -> mlFeatureTableMap.getOrDefault(featureTableUrn, null))
                .collect(Collectors.toList());

            return gmsResults.stream()
                .map(gmsMlFeatureTable -> gmsMlFeatureTable == null ? null
                    : DataFetcherResult.<MLFeatureTable>newResult()
                        .data(MLFeatureTableMapper.map(gmsMlFeatureTable))
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
        final SearchResult searchResult = _entityClient.search("mlFeatureTable", query, facetFilters, start, count, context.getAuthentication());
        return UrnSearchResultsMapper.map(searchResult);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit,
                                            @Nonnull final QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final AutoCompleteResult result = _entityClient.autoComplete("mlFeatureTable", query, facetFilters, limit, context.getAuthentication());
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
        final BrowseResult result = _entityClient.browse(
                "mlFeatureTable",
                pathStr,
                facetFilters,
                start,
                count,
            context.getAuthentication());
        return BrowseResultMapper.map(result);
    }

    @Override
    public List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull final QueryContext context) throws Exception {
        final StringArray result = _entityClient.getBrowsePaths(MLModelUtils.getUrn(urn), context.getAuthentication());
        return BrowsePathsMapper.map(result);
    }
}
