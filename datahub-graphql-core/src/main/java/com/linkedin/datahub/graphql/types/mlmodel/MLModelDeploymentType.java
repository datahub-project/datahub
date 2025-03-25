package com.linkedin.datahub.graphql.types.mlmodel;

import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.MLModelDeployment;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowseResultMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.MLModelDeploymentMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchResult;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class MLModelDeploymentType
    implements SearchableEntityType<MLModelDeployment, String>,
        BrowsableEntityType<MLModelDeployment, String> {

  private static final Set<String> ASPECTS_TO_RESOLVE =
      ImmutableSet.of(
          "mlModelDeploymentKey",
          "mlModelDeploymentProperties",
          OWNERSHIP_ASPECT_NAME,
          STATUS_ASPECT_NAME,
          DEPRECATION_ASPECT_NAME,
          GLOBAL_TAGS_ASPECT_NAME,
          DATA_PLATFORM_INSTANCE_ASPECT_NAME);

  private static final Set<String> FACET_FIELDS = ImmutableSet.of("platform");
  private static final String ENTITY_NAME = "mlModelDeployment";

  private final EntityClient _entityClient;

  public MLModelDeploymentType(@Nonnull final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public Class<MLModelDeployment> objectClass() {
    return MLModelDeployment.class;
  }

  @Override
  public EntityType type() {
    return EntityType.valueOf("MLMODEL_DEPLOYMENT");
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public List<DataFetcherResult<MLModelDeployment>> batchLoad(
      @Nonnull final List<String> urnStrs, @Nonnull final QueryContext context) {
    try {
      final List<Urn> urns = urnStrs.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

      final Map<Urn, EntityResponse> mlModelDeploymentMap =
          _entityClient.batchGetV2(
              context.getOperationContext(), ENTITY_NAME, new HashSet<>(urns), ASPECTS_TO_RESOLVE);

      final List<EntityResponse> gmsResults = new ArrayList<>(urnStrs.size());
      for (Urn urn : urns) {
        gmsResults.add(mlModelDeploymentMap.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(
              gmsMLModelDeployment ->
                  gmsMLModelDeployment == null
                      ? null
                      : DataFetcherResult.<MLModelDeployment>newResult()
                          .data(MLModelDeploymentMapper.map(gmsMLModelDeployment))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load MLModelDeployments", e);
    }
  }

  @Override
  public SearchResults search(
      @Nonnull String query,
      @Nullable List<FacetFilterInput> filters,
      int start,
      int count,
      @Nonnull final QueryContext context)
      throws Exception {
    final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
    final SearchResult searchResult =
        _entityClient.search(
            context.getOperationContext(), ENTITY_NAME, query, facetFilters, start, count);
    return UrnSearchResultsMapper.map(context, searchResult);
  }

  @Override
  public AutoCompleteResults autoComplete(
      @Nonnull String query,
      @Nullable String field,
      @Nullable Filter filters,
      int limit,
      @Nonnull final QueryContext context)
      throws Exception {
    final AutoCompleteResult result =
        _entityClient.autoComplete(
            context.getOperationContext(), ENTITY_NAME, query, filters, limit);
    return AutoCompleteResultsMapper.map(context, result);
  }

  @Override
  public BrowseResults browse(
      @Nonnull List<String> path,
      @Nullable List<FacetFilterInput> filters,
      int start,
      int count,
      @Nonnull final QueryContext context)
      throws Exception {
    final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
    final String pathStr = path.size() > 0 ? "/" + String.join("/", path) : "";
    final BrowseResult result =
        _entityClient.browse(
            context.getOperationContext(), ENTITY_NAME, pathStr, facetFilters, start, count);
    return BrowseResultMapper.map(context, result);
  }

  @Override
  public List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull final QueryContext context)
      throws Exception {
    return BrowsePathsMapper.map(
        context, _entityClient.getBrowsePaths(context.getOperationContext(), UrnUtils.getUrn(urn)));
  }
}
