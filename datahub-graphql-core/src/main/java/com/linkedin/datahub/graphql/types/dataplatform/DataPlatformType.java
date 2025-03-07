package com.linkedin.datahub.graphql.types.dataplatform;

import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.EntityType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.dataplatform.mappers.DataPlatformMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchResult;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DataPlatformType
    implements SearchableEntityType<DataPlatform, String>, EntityType<DataPlatform, String> {

  private final EntityClient _entityClient;

  public DataPlatformType(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public Class<DataPlatform> objectClass() {
    return DataPlatform.class;
  }

  @Override
  public List<DataFetcherResult<DataPlatform>> batchLoad(
      final List<String> urns, final QueryContext context) {

    final List<Urn> dataPlatformUrns =
        urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    try {
      final Map<Urn, EntityResponse> dataPlatformMap =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              DATA_PLATFORM_ENTITY_NAME,
              new HashSet<>(dataPlatformUrns),
              null);

      final List<EntityResponse> gmsResults = new ArrayList<>();
      for (Urn urn : dataPlatformUrns) {
        gmsResults.add(dataPlatformMap.getOrDefault(urn, null));
      }

      return gmsResults.stream()
          .map(
              gmsPlatform ->
                  gmsPlatform == null
                      ? null
                      : DataFetcherResult.<DataPlatform>newResult()
                          .data(DataPlatformMapper.map(context, gmsPlatform))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Data Platforms", e);
    }
  }

  @Override
  public com.linkedin.datahub.graphql.generated.EntityType type() {
    return com.linkedin.datahub.graphql.generated.EntityType.DATA_PLATFORM;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public SearchResults search(
      @Nonnull String query,
      @Nullable List<FacetFilterInput> filters,
      int start,
      int count,
      @Nonnull final QueryContext context)
      throws Exception {
    final Map<String, String> facetFilters =
        ResolverUtils.buildFacetFilters(filters, ImmutableSet.of());
    final SearchResult searchResult =
        _entityClient.search(
            context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
            DATA_PLATFORM_ENTITY_NAME,
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
      int limit,
      @Nonnull final QueryContext context)
      throws Exception {
    final AutoCompleteResult result =
        _entityClient.autoComplete(
            context.getOperationContext(), DATA_PLATFORM_ENTITY_NAME, query, filters, limit);
    return AutoCompleteResultsMapper.map(context, result);
  }
}
