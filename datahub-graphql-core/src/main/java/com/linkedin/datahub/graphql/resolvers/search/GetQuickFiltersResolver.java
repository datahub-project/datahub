package com.linkedin.datahub.graphql.resolvers.search;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.GetQuickFiltersInput;
import com.linkedin.datahub.graphql.generated.GetQuickFiltersResult;
import com.linkedin.datahub.graphql.generated.QuickFilter;
import com.linkedin.datahub.graphql.resolvers.EntityTypeMapper;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.FilterValue;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.view.DataHubViewInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.SEARCHABLE_ENTITY_TYPES;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.resolveView;
import static com.linkedin.metadata.Constants.*;

@Slf4j
@RequiredArgsConstructor
public class GetQuickFiltersResolver implements DataFetcher<CompletableFuture<GetQuickFiltersResult>> {

  private final EntityClient _entityClient;
  private final ViewService _viewService;

  private static final String PLATFORM = "platform";
  private static final int PLATFORM_COUNT = 5;
  private static final String ENTITY = "entity";
  private static final int SOURCE_ENTITY_COUNT = 3;
  private static final int DATAHUB_ENTITY_COUNT = 2;

  private static final List<String> PRIORITIZED_SOURCE_ENTITY_TYPES = Stream.of(
      DATASET_ENTITY_NAME,
      DASHBOARD_ENTITY_NAME,
      DATA_FLOW_ENTITY_NAME,
      DATA_JOB_ENTITY_NAME,
      CHART_ENTITY_NAME,
      CONTAINER_ENTITY_NAME,
      ML_MODEL_ENTITY_NAME,
      ML_MODEL_GROUP_ENTITY_NAME,
      ML_FEATURE_ENTITY_NAME,
      ML_FEATURE_TABLE_ENTITY_NAME,
      ML_PRIMARY_KEY_ENTITY_NAME
  ).map(String::toLowerCase).collect(Collectors.toList());

  private static final List<String> PRIORITIZED_DATAHUB_ENTITY_TYPES = Stream.of(
      DOMAIN_ENTITY_NAME,
      GLOSSARY_TERM_ENTITY_NAME,
      CORP_GROUP_ENTITY_NAME,
      CORP_USER_ENTITY_NAME
  ).map(String::toLowerCase).collect(Collectors.toList());


  public CompletableFuture<GetQuickFiltersResult> get(final DataFetchingEnvironment environment) throws Exception {
    final GetQuickFiltersInput input =  bindArgument(environment.getArgument("input"), GetQuickFiltersInput.class);

    return CompletableFuture.supplyAsync(() -> {
      GetQuickFiltersResult result = new GetQuickFiltersResult();
      List<QuickFilter> quickFilters = new ArrayList<>();

      try {
        SearchResult searchResult = getSearchResults(environment, input);
        AggregationMetadataArray aggregations = searchResult.getMetadata().getAggregations();

        quickFilters.addAll(getPlatformQuickFilters(aggregations));
        quickFilters.addAll(getEntityTypeQuickFilters(aggregations));
      } catch (Exception e) {
        log.error("Failed getting quick filters", e);
        throw new RuntimeException("Failed to to get quick filters", e);
      }

      result.setQuickFilters(quickFilters);
      return result;
    });
  }

  /**
   * Do a star search with view filter applied to get info about all data in this instance.
   */
  private SearchResult getSearchResults(@Nonnull final DataFetchingEnvironment environment, @Nonnull final GetQuickFiltersInput input) throws Exception {
    final QueryContext context = environment.getContext();
    final DataHubViewInfo maybeResolvedView = (input.getViewUrn() != null)
        ? resolveView(_viewService, UrnUtils.getUrn(input.getViewUrn()), context.getAuthentication())
        : null;
    final List<String> entityNames = SEARCHABLE_ENTITY_TYPES.stream().map(EntityTypeMapper::getName).collect(Collectors.toList());

    return _entityClient.searchAcrossEntities(
        maybeResolvedView != null
            ? SearchUtils.intersectEntityTypes(entityNames, maybeResolvedView.getDefinition().getEntityTypes())
            : entityNames,
        "*",
        maybeResolvedView != null
            ? SearchUtils.combineFilters(null, maybeResolvedView.getDefinition().getFilter())
            : null,
        0,
        0,
        null,
        ResolverUtils.getAuthentication(environment));
  }

  /**
   * Get platforms and their count from an aggregations array, sorts by entity count, and map the top 5 to quick filters
   */
  private List<QuickFilter> getPlatformQuickFilters(@Nonnull final AggregationMetadataArray aggregations) {
    List<QuickFilter> platforms = new ArrayList<>();
    Optional<AggregationMetadata> platformAggregations = aggregations.stream().filter(agg -> agg.getName().equals(PLATFORM)).findFirst();
    if (platformAggregations.isPresent()) {
      List<FilterValue> sortedPlatforms =
          platformAggregations.get().getFilterValues().stream().sorted(Comparator.comparingLong(val -> -val.getFacetCount())).collect(Collectors.toList());
      sortedPlatforms.forEach(platformFilter -> {
        if (platforms.size() < PLATFORM_COUNT && platformFilter.getFacetCount() > 0) {
          platforms.add(mapQuickFilter(PLATFORM, platformFilter));
        }
      });
    }

    return platforms;
  }

  /**
   * Gets entity type quick filters from search aggregations. First, get source entity type quick filters
   * from a prioritized list. Do the same for datathub entity types.
   */
  private List<QuickFilter> getEntityTypeQuickFilters(@Nonnull final AggregationMetadataArray aggregations) {
    List<QuickFilter> entityTypes = new ArrayList<>();
    Optional<AggregationMetadata> entityAggregations = aggregations.stream().filter(agg -> agg.getName().equals(ENTITY)).findFirst();

    if (entityAggregations.isPresent()) {
      List<QuickFilter> sourceEntityTypeFilters = getQuickFiltersFromList(PRIORITIZED_SOURCE_ENTITY_TYPES, SOURCE_ENTITY_COUNT, entityAggregations.get());
      entityTypes.addAll(sourceEntityTypeFilters);

      List<QuickFilter> dataHubEntityTypeFilters = getQuickFiltersFromList(PRIORITIZED_DATAHUB_ENTITY_TYPES, DATAHUB_ENTITY_COUNT, entityAggregations.get());
      entityTypes.addAll(dataHubEntityTypeFilters);
    }
    return entityTypes;
  }

  /**
   * Create a quick filters list by looping over prioritized list and adding filters that exist until we reach the maxListSize defined
   */
  private List<QuickFilter> getQuickFiltersFromList(
      @Nonnull final List<String> prioritizedList,
      final int maxListSize,
      @Nonnull final AggregationMetadata entityAggregations
  ) {
    List<QuickFilter> entityTypes = new ArrayList<>();
    prioritizedList.forEach(entityType -> {
      if (entityTypes.size() < maxListSize) {
        Optional<FilterValue> entityFilter = entityAggregations.getFilterValues().stream().filter(val -> val.getValue().equals(entityType)).findFirst();
        if (entityFilter.isPresent() && entityFilter.get().getFacetCount() > 0) {
          entityTypes.add(mapQuickFilter(ENTITY, entityFilter.get()));
        }
      }
    });

    return entityTypes;
  }

  private QuickFilter mapQuickFilter(@Nonnull final String field, @Nonnull final FilterValue filterValue) {
    boolean isEntityTypeFilter = field.equals(ENTITY);
    QuickFilter quickFilter = new QuickFilter();
    quickFilter.setField(field);
    quickFilter.setValue(convertFilterValue(filterValue.getValue(), isEntityTypeFilter));
    if (filterValue.getEntity() != null) {
      Entity entity = UrnToEntityMapper.map(filterValue.getEntity());
      quickFilter.setEntity(entity);
    }
    return quickFilter;
  }

  /**
   * If we're working with an entity type filter, we need to convert the value to an EntityType
   */
  public static String convertFilterValue(String filterValue, boolean isEntityType) {
    if (isEntityType) {
      return EntityTypeMapper.getType(filterValue).toString();
    }
    return filterValue;
  }
}
