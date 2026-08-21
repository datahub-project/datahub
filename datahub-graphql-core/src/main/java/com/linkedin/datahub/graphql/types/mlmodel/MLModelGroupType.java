package com.linkedin.datahub.graphql.types.mlmodel;

import static com.linkedin.datahub.graphql.Constants.*;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.Constants.ML_MODEL_GROUP_KEY_ASPECT_NAME;

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
import com.linkedin.datahub.graphql.generated.MLModelGroup;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowseResultMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.MLModelGroupMapper;
import com.linkedin.datahub.graphql.util.AspectUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Filter;
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

public class MLModelGroupType
    implements SearchableEntityType<MLModelGroup, String>,
        BrowsableEntityType<MLModelGroup, String> {
  static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(
          ML_MODEL_GROUP_KEY_ASPECT_NAME,
          ML_MODEL_GROUP_PROPERTIES_ASPECT_NAME,
          ML_MODEL_GROUP_EDITABLE_PROPERTIES_ASPECT_NAME,
          OWNERSHIP_ASPECT_NAME,
          STATUS_ASPECT_NAME,
          GLOBAL_TAGS_ASPECT_NAME,
          GLOSSARY_TERMS_ASPECT_NAME,
          DOMAINS_ASPECT_NAME,
          DEPRECATION_ASPECT_NAME,
          INSTITUTIONAL_MEMORY_ASPECT_NAME,
          DATA_PLATFORM_INSTANCE_ASPECT_NAME,
          BROWSE_PATHS_V2_ASPECT_NAME,
          STRUCTURED_PROPERTIES_ASPECT_NAME,
          FORMS_ASPECT_NAME,
          APPLICATION_MEMBERSHIP_ASPECT_NAME);

  private static final Set<String> FACET_FIELDS = ImmutableSet.of("origin", "platform");
  private final EntityClient _entityClient;

  public MLModelGroupType(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public EntityType type() {
    return EntityType.MLMODEL_GROUP;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<MLModelGroup> objectClass() {
    return MLModelGroup.class;
  }

  @Override
  public List<DataFetcherResult<MLModelGroup>> batchLoad(
      final List<String> urns, @Nonnull final QueryContext context) throws Exception {
    final List<Urn> mlModelGroupUrns =
        urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    try {
      Set<String> aspectsToResolve =
          AspectUtils.getOptimizedAspects(
              context, name(), ASPECTS_TO_FETCH, ML_MODEL_GROUP_KEY_ASPECT_NAME);
      final Map<Urn, EntityResponse> mlModelMap =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              ML_MODEL_GROUP_ENTITY_NAME,
              new HashSet<>(mlModelGroupUrns),
              aspectsToResolve);

      final List<EntityResponse> gmsResults =
          mlModelGroupUrns.stream()
              .map(modelUrn -> mlModelMap.getOrDefault(modelUrn, null))
              .collect(Collectors.toList());

      return gmsResults.stream()
          .map(
              gmsMlModelGroup ->
                  gmsMlModelGroup == null
                      ? null
                      : DataFetcherResult.<MLModelGroup>newResult()
                          .data(MLModelGroupMapper.map(context, gmsMlModelGroup))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load MLModelGroups", e);
    }
  }

  @Override
  public SearchResults search(
      @Nonnull String query,
      @Nullable List<FacetFilterInput> filters,
      int start,
      @Nullable Integer count,
      @Nonnull final QueryContext context)
      throws Exception {
    final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
    final SearchResult searchResult =
        _entityClient.search(
            context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
            "mlModelGroup",
            query,
            facetFilters,
            start,
            count);
    return UrnSearchResultsMapper.map(context, searchResult);
  }

  @Override
  public AutoCompleteResults autoComplete(
      @Nonnull String query,
      @Nullable String field,
      @Nullable Filter filters,
      @Nullable Integer limit,
      @Nonnull final QueryContext context)
      throws Exception {
    final AutoCompleteResult result =
        _entityClient.autoComplete(
            context.getOperationContext(), "mlModelGroup", query, filters, limit);
    return AutoCompleteResultsMapper.map(context, result);
  }

  @Override
  public BrowseResults browse(
      @Nonnull List<String> path,
      @Nullable List<FacetFilterInput> filters,
      int start,
      @Nullable Integer count,
      @Nonnull final QueryContext context)
      throws Exception {
    final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
    final String pathStr =
        path.size() > 0 ? BROWSE_PATH_DELIMITER + String.join(BROWSE_PATH_DELIMITER, path) : "";
    final BrowseResult result =
        _entityClient.browse(
            context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(false)),
            "mlModelGroup",
            pathStr,
            facetFilters,
            start,
            count);
    return BrowseResultMapper.map(context, result);
  }

  @Override
  public List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull final QueryContext context)
      throws Exception {
    final StringArray result =
        _entityClient.getBrowsePaths(
            context.getOperationContext(), MLModelUtils.getMLModelGroupUrn(urn));
    return BrowsePathsMapper.map(context, result);
  }
}
