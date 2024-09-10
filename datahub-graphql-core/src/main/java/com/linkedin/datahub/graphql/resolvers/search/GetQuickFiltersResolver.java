package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.datahub.graphql.Constants.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.SEARCHABLE_ENTITY_TYPES;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.resolveView;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.GetQuickFiltersInput;
import com.linkedin.datahub.graphql.generated.GetQuickFiltersResult;
import com.linkedin.datahub.graphql.generated.QuickFilter;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.FilterValue;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.view.DataHubViewInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class GetQuickFiltersResolver
    implements DataFetcher<CompletableFuture<GetQuickFiltersResult>> {

  private final EntityClient _entityClient;
  private final ViewService _viewService;

  private static final String PLATFORM = "platform";
  private static final int PLATFORM_COUNT = 5;
  private static final int SOURCE_ENTITY_COUNT = 3;
  private static final int DATAHUB_ENTITY_COUNT = 2;

  public CompletableFuture<GetQuickFiltersResult> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final GetQuickFiltersInput input =
        bindArgument(environment.getArgument("input"), GetQuickFiltersInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final GetQuickFiltersResult result = new GetQuickFiltersResult();
          final List<QuickFilter> quickFilters = new ArrayList<>();

          try {
            final SearchResult searchResult =
                getSearchResults(context.getOperationContext(), input);
            final AggregationMetadataArray aggregations =
                searchResult.getMetadata().getAggregations();

            quickFilters.addAll(getPlatformQuickFilters(context, aggregations));
            quickFilters.addAll(getEntityTypeQuickFilters(context, aggregations));
          } catch (Exception e) {
            log.error("Failed getting quick filters", e);
            throw new RuntimeException("Failed to to get quick filters", e);
          }

          result.setQuickFilters(quickFilters);
          return result;
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Do a star search with view filter applied to get info about all data in this instance. Include
   * aggregations.
   */
  private SearchResult getSearchResults(
      @Nonnull final OperationContext opContext, @Nonnull final GetQuickFiltersInput input)
      throws Exception {
    final DataHubViewInfo maybeResolvedView =
        (input.getViewUrn() != null)
            ? resolveView(opContext, _viewService, UrnUtils.getUrn(input.getViewUrn()))
            : null;
    final List<String> entityNames =
        SEARCHABLE_ENTITY_TYPES.stream()
            .map(EntityTypeMapper::getName)
            .collect(Collectors.toList());

    return _entityClient.searchAcrossEntities(
        opContext.withSearchFlags(flags -> flags.setSkipAggregates(false)),
        maybeResolvedView != null
            ? SearchUtils.intersectEntityTypes(
                entityNames, maybeResolvedView.getDefinition().getEntityTypes())
            : entityNames,
        "*",
        maybeResolvedView != null
            ? SearchUtils.combineFilters(null, maybeResolvedView.getDefinition().getFilter())
            : null,
        0,
        0,
        Collections.emptyList(),
        null);
  }

  /**
   * Get platforms and their count from an aggregations array, sorts by entity count, and map the
   * top 5 to quick filters
   */
  private List<QuickFilter> getPlatformQuickFilters(
      @Nullable QueryContext context, @Nonnull final AggregationMetadataArray aggregations) {
    final List<QuickFilter> platforms = new ArrayList<>();
    final Optional<AggregationMetadata> platformAggregations =
        aggregations.stream().filter(agg -> agg.getName().equals(PLATFORM)).findFirst();
    if (platformAggregations.isPresent()) {
      final List<FilterValue> sortedPlatforms =
          platformAggregations.get().getFilterValues().stream()
              .sorted(Comparator.comparingLong(val -> -val.getFacetCount()))
              .collect(Collectors.toList());
      sortedPlatforms.forEach(
          platformFilter -> {
            if (platforms.size() < PLATFORM_COUNT && platformFilter.getFacetCount() > 0) {
              platforms.add(mapQuickFilter(context, PLATFORM, platformFilter));
            }
          });
    }

    // return platforms sorted alphabetically by their name
    return platforms.stream()
        .sorted(Comparator.comparing(QuickFilter::getValue))
        .collect(Collectors.toList());
  }

  /**
   * Gets entity type quick filters from search aggregations. First, get source entity type quick
   * filters from a prioritized list. Do the same for datathub entity types.
   */
  private List<QuickFilter> getEntityTypeQuickFilters(
      @Nullable QueryContext context, @Nonnull final AggregationMetadataArray aggregations) {
    final List<QuickFilter> entityTypes = new ArrayList<>();
    final Optional<AggregationMetadata> entityAggregations =
        aggregations.stream().filter(agg -> agg.getName().equals(ENTITY_FILTER_NAME)).findFirst();

    if (entityAggregations.isPresent()) {
      final List<QuickFilter> sourceEntityTypeFilters =
          getQuickFiltersFromList(
              context,
              SearchUtils.PRIORITIZED_SOURCE_ENTITY_TYPES,
              SOURCE_ENTITY_COUNT,
              entityAggregations.get());
      entityTypes.addAll(sourceEntityTypeFilters);

      final List<QuickFilter> dataHubEntityTypeFilters =
          getQuickFiltersFromList(
              context,
              SearchUtils.PRIORITIZED_DATAHUB_ENTITY_TYPES,
              DATAHUB_ENTITY_COUNT,
              entityAggregations.get());
      entityTypes.addAll(dataHubEntityTypeFilters);
    }
    return entityTypes;
  }

  /**
   * Create a quick filters list by looping over prioritized list and adding filters that exist
   * until we reach the maxListSize defined
   */
  private List<QuickFilter> getQuickFiltersFromList(
      @Nullable QueryContext context,
      @Nonnull final List<String> prioritizedList,
      final int maxListSize,
      @Nonnull final AggregationMetadata entityAggregations) {
    final List<QuickFilter> entityTypes = new ArrayList<>();
    prioritizedList.forEach(
        entityType -> {
          if (entityTypes.size() < maxListSize) {
            final Optional<FilterValue> entityFilter =
                entityAggregations.getFilterValues().stream()
                    .filter(val -> val.getValue().equals(entityType))
                    .findFirst();
            if (entityFilter.isPresent() && entityFilter.get().getFacetCount() > 0) {
              entityTypes.add(mapQuickFilter(context, ENTITY_FILTER_NAME, entityFilter.get()));
            }
          }
        });

    return entityTypes;
  }

  private QuickFilter mapQuickFilter(
      @Nullable QueryContext context,
      @Nonnull final String field,
      @Nonnull final FilterValue filterValue) {
    final boolean isEntityTypeFilter = field.equals(ENTITY_FILTER_NAME);
    final QuickFilter quickFilter = new QuickFilter();
    quickFilter.setField(field);
    quickFilter.setValue(convertFilterValue(filterValue.getValue(), isEntityTypeFilter));
    if (filterValue.getEntity() != null) {
      final Entity entity = UrnToEntityMapper.map(context, filterValue.getEntity());
      quickFilter.setEntity(entity);
    }
    return quickFilter;
  }

  /** If we're working with an entity type filter, we need to convert the value to an EntityType */
  public static String convertFilterValue(String filterValue, boolean isEntityType) {
    if (isEntityType) {
      return EntityTypeMapper.getType(filterValue).toString();
    }
    return filterValue;
  }
}
