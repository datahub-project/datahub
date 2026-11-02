package com.linkedin.datahub.graphql.types.mlmodel;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.Constants.ML_PRIMARY_KEY_KEY_ASPECT_NAME;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.MLPrimaryKey;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.MLPrimaryKeyMapper;
import com.linkedin.datahub.graphql.util.AspectUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
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

public class MLPrimaryKeyType implements SearchableEntityType<MLPrimaryKey, String> {
  static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(
          ML_PRIMARY_KEY_KEY_ASPECT_NAME,
          ML_PRIMARY_KEY_PROPERTIES_ASPECT_NAME,
          ML_PRIMARY_KEY_EDITABLE_PROPERTIES_ASPECT_NAME,
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

  private static final Set<String> FACET_FIELDS = ImmutableSet.of("");
  private final EntityClient _entityClient;

  public MLPrimaryKeyType(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public EntityType type() {
    return EntityType.MLPRIMARY_KEY;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<MLPrimaryKey> objectClass() {
    return MLPrimaryKey.class;
  }

  @Override
  public List<DataFetcherResult<MLPrimaryKey>> batchLoad(
      final List<String> urns, @Nonnull final QueryContext context) throws Exception {

    final List<Urn> mlPrimaryKeyUrns =
        urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    try {
      Set<String> aspectsToResolve =
          AspectUtils.getOptimizedAspects(
              context, name(), ASPECTS_TO_FETCH, ML_PRIMARY_KEY_KEY_ASPECT_NAME);
      final Map<Urn, EntityResponse> mlPrimaryKeyMap =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              ML_PRIMARY_KEY_ENTITY_NAME,
              new HashSet<>(mlPrimaryKeyUrns),
              aspectsToResolve);

      final List<EntityResponse> gmsResults =
          mlPrimaryKeyUrns.stream()
              .map(primaryKeyUrn -> mlPrimaryKeyMap.getOrDefault(primaryKeyUrn, null))
              .collect(Collectors.toList());

      return gmsResults.stream()
          .map(
              gmsMlPrimaryKey ->
                  gmsMlPrimaryKey == null
                      ? null
                      : DataFetcherResult.<MLPrimaryKey>newResult()
                          .data(MLPrimaryKeyMapper.map(context, gmsMlPrimaryKey))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load MLPrimaryKeys", e);
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
            "mlPrimaryKey",
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
            context.getOperationContext(), "mlPrimaryKey", query, filters, limit);
    return AutoCompleteResultsMapper.map(context, result);
  }
}
