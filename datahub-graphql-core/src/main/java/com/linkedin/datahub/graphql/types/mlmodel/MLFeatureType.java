package com.linkedin.datahub.graphql.types.mlmodel;

import com.google.common.collect.ImmutableSet;

import com.linkedin.common.urn.MLFeatureUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.MLFeature;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.MLFeatureSnapshotMapper;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.extractor.AspectExtractor;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.search.SearchResult;
import graphql.execution.DataFetcherResult;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class MLFeatureType implements SearchableEntityType<MLFeature> {

    private static final Set<String> FACET_FIELDS = ImmutableSet.of("");
    private final EntityClient _entityClient;

    public MLFeatureType(final EntityClient entityClient) {
        _entityClient = entityClient;
    }

    @Override
    public EntityType type() {
        return EntityType.MLFEATURE;
    }

    @Override
    public Class<MLFeature> objectClass() {
        return MLFeature.class;
    }

    @Override
    public List<DataFetcherResult<MLFeature>> batchLoad(final List<String> urns, final QueryContext context) throws Exception {
        final List<MLFeatureUrn> mlFeatureUrns = urns.stream()
            .map(MLModelUtils::getMLFeatureUrn)
            .collect(Collectors.toList());

        try {
            final Map<Urn, Entity> mlFeatureMap = _entityClient.batchGet(mlFeatureUrns
                .stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()),
            context.getActor());

            final List<Entity> gmsResults = mlFeatureUrns.stream()
                .map(featureUrn -> mlFeatureMap.getOrDefault(featureUrn, null)).collect(Collectors.toList());

            return gmsResults.stream()
                .map(gmsMlFeature -> gmsMlFeature == null ? null
                    : DataFetcherResult.<MLFeature>newResult()
                        .data(MLFeatureSnapshotMapper.map(gmsMlFeature.getValue().getMLFeatureSnapshot()))
                        .localContext(AspectExtractor.extractAspects(gmsMlFeature.getValue().getMLFeatureSnapshot()))
                        .build())
                .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load MLFeatures", e);
        }
    }

    @Override
    public SearchResults search(@Nonnull String query,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count,
                                @Nonnull final QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final SearchResult searchResult = _entityClient.search("mlFeature", query, facetFilters, start, count, context.getActor());
        return UrnSearchResultsMapper.map(searchResult);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit,
                                            @Nonnull final QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final AutoCompleteResult result = _entityClient.autoComplete("mlFeature", query, facetFilters, limit, context.getActor());
        return AutoCompleteResultsMapper.map(result);
    }
}
