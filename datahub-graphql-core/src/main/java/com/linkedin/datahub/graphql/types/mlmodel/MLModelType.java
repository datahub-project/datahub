package com.linkedin.datahub.graphql.types.mlmodel;

import com.linkedin.common.urn.Urn;


import com.linkedin.datahub.graphql.types.mappers.BrowseResultMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.MLModelSnapshotMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.extractor.AspectExtractor;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.browse.BrowseResult;
import graphql.execution.DataFetcherResult;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import com.linkedin.data.template.StringArray;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.MLModelUrn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.MLModel;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.metadata.query.AutoCompleteResult;
import static com.linkedin.datahub.graphql.Constants.BROWSE_PATH_DELIMITER;

public class MLModelType implements SearchableEntityType<MLModel>, BrowsableEntityType<MLModel> {

    private static final Set<String> FACET_FIELDS = ImmutableSet.of("origin", "platform");
    private final EntityClient _entityClient;

    public MLModelType(final EntityClient entityClient) {
        _entityClient = entityClient;
    }

    @Override
    public EntityType type() {
        return EntityType.MLMODEL;
    }

    @Override
    public Class<MLModel> objectClass() {
        return MLModel.class;
    }

    @Override
    public List<DataFetcherResult<MLModel>> batchLoad(final List<String> urns, final QueryContext context) throws Exception {
        final List<MLModelUrn> mlModelUrns = urns.stream()
            .map(MLModelUtils::getMLModelUrn)
            .collect(Collectors.toList());

        try {
            final Map<Urn, Entity> mlModelMap = _entityClient.batchGet(mlModelUrns
                .stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()),
            context.getActor());

            final List<Entity> gmsResults = mlModelUrns.stream()
                .map(modelUrn -> mlModelMap.getOrDefault(modelUrn, null)).collect(Collectors.toList());

            return gmsResults.stream()
                .map(gmsMlModel -> gmsMlModel == null ? null
                    : DataFetcherResult.<MLModel>newResult()
                        .data(MLModelSnapshotMapper.map(gmsMlModel.getValue().getMLModelSnapshot()))
                        .localContext(AspectExtractor.extractAspects(gmsMlModel.getValue().getMLModelSnapshot()))
                        .build())
                .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load MLModels", e);
        }
    }

    @Override
    public SearchResults search(@Nonnull String query,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count,
                                @Nonnull final QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final SearchResult searchResult = _entityClient.search("mlModel", query, facetFilters, start, count, context.getActor());
        return UrnSearchResultsMapper.map(searchResult);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit,
                                            @Nonnull final QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final AutoCompleteResult result = _entityClient.autoComplete("mlModel", query, facetFilters, limit, context.getActor());
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
                "mlModel",
                pathStr,
                facetFilters,
                start,
                count,
            context.getActor());
        return BrowseResultMapper.map(result);
    }

    @Override
    public List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull final QueryContext context) throws Exception {
        final StringArray result = _entityClient.getBrowsePaths(MLModelUtils.getMLModelUrn(urn), context.getActor());
        return BrowsePathsMapper.map(result);
    }
}
