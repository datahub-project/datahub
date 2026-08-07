package com.linkedin.datahub.graphql.resolvers.metrics;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.search.utils.QueryUtils.buildFilterWithUrns;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.SearchAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.generated.SemanticModel;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.common.mappers.SearchFlagsInputMapper;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.semanticmodel.SemanticModelInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver for SemanticModel.entities: reads membership from semanticModelInfo.datasets / .metrics,
 * then searches those URNs. Mirrors ListDataProductAssetsResolver for the lineage-explorer
 * container pattern.
 */
@Slf4j
@RequiredArgsConstructor
public class ListSemanticModelEntitiesResolver
    implements DataFetcher<CompletableFuture<SearchResults>> {

  private static final int DEFAULT_START = 0;
  private static final int DEFAULT_COUNT = 10;

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<SearchResults> get(DataFetchingEnvironment environment) {
    final QueryContext context = ResolverUtils.getQueryContext(environment);
    final String urn =
        environment.getArgument("urn") != null
            ? environment.getArgument("urn")
            : ((SemanticModel) environment.getSource()).getUrn();
    final Urn semanticModelUrn = UrnUtils.getUrn(urn);
    final SearchAcrossEntitiesInput input =
        bindArgument(environment.getArgument("input"), SearchAcrossEntitiesInput.class);

    final List<Urn> memberUrns = new ArrayList<>();
    try {
      final EntityResponse entityResponse =
          _entityClient.getV2(
              context.getOperationContext(),
              Constants.SEMANTIC_MODEL_ENTITY_NAME,
              semanticModelUrn,
              Collections.singleton(Constants.SEMANTIC_MODEL_INFO_ASPECT_NAME),
              false);
      if (entityResponse != null
          && entityResponse.getAspects().containsKey(Constants.SEMANTIC_MODEL_INFO_ASPECT_NAME)) {
        final DataMap data =
            entityResponse
                .getAspects()
                .get(Constants.SEMANTIC_MODEL_INFO_ASPECT_NAME)
                .getValue()
                .data();
        final SemanticModelInfo info = new SemanticModelInfo(data);
        if (info.hasDatasets() && info.getDatasets() != null) {
          memberUrns.addAll(info.getDatasets());
        }
        if (info.hasMetrics() && info.getMetrics() != null) {
          memberUrns.addAll(info.getMetrics());
        }
      }
    } catch (Exception e) {
      log.error(
          String.format("Failed to list semantic model entities with urn %s", semanticModelUrn), e);
      throw new RuntimeException(
          String.format("Failed to list semantic model entities with urn %s", semanticModelUrn), e);
    }

    final List<String> entitiesToQuery =
        memberUrns.stream().map(Urn::getEntityType).distinct().collect(Collectors.toList());

    final List<EntityType> inputEntityTypes =
        (input.getTypes() == null || input.getTypes().isEmpty())
            ? ImmutableList.of()
            : input.getTypes();
    final List<String> inputEntityNames =
        inputEntityTypes.stream()
            .map(EntityTypeMapper::getName)
            .distinct()
            .collect(Collectors.toList());

    final List<String> finalEntityNames =
        !inputEntityNames.isEmpty() ? inputEntityNames : entitiesToQuery;

    final String sanitizedQuery = ResolverUtils.escapeForwardSlash(input.getQuery());
    final int start = input.getStart() != null ? input.getStart() : DEFAULT_START;
    final int count = input.getCount() != null ? input.getCount() : DEFAULT_COUNT;

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (memberUrns.isEmpty()) {
            SearchResults results = new SearchResults();
            results.setStart(start);
            results.setCount(count);
            results.setTotal(0);
            results.setSearchResults(ImmutableList.of());
            return results;
          }

          final Filter baseFilter =
              ResolverUtils.buildFilter(input.getFilters(), input.getOrFilters());
          final Filter finalFilter =
              buildFilterWithUrns(
                  context.getDataHubAppConfig(), new HashSet<>(memberUrns), baseFilter);

          final SearchFlags searchFlags;
          com.linkedin.datahub.graphql.generated.SearchFlags inputFlags = input.getSearchFlags();
          if (inputFlags != null) {
            searchFlags = SearchFlagsInputMapper.INSTANCE.apply(context, inputFlags);
          } else {
            searchFlags = null;
          }

          try {
            return UrnSearchResultsMapper.map(
                context,
                _entityClient.searchAcrossEntities(
                    context
                        .getOperationContext()
                        .withSearchFlags(flags -> searchFlags != null ? searchFlags : flags),
                    finalEntityNames,
                    sanitizedQuery,
                    finalFilter,
                    start,
                    count,
                    null));
          } catch (Exception e) {
            log.error(
                "Failed to execute search for semantic model entities: entity types {}, query {}, start: {}, count: {}",
                input.getTypes(),
                input.getQuery(),
                start,
                count);
            throw new RuntimeException(
                "Failed to execute search for semantic model entities: "
                    + String.format(
                        "entity types %s, query %s, start: %s, count: %s",
                        input.getTypes(), input.getQuery(), start, count),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
