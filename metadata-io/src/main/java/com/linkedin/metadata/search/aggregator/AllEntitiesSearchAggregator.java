package com.linkedin.metadata.search.aggregator;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.linkedin.data.template.LongMap;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.cache.EntitySearchServiceCache;
import com.linkedin.metadata.search.cache.NonEmptyEntitiesCache;
import com.linkedin.metadata.search.ranker.SearchRanker;
import com.linkedin.metadata.utils.ConcurrencyUtils;
import com.linkedin.metadata.utils.SearchUtil;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.util.Pair;
import io.opentelemetry.extension.annotations.WithSpan;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.CacheManager;


@Slf4j
public class AllEntitiesSearchAggregator {
  private final EntityRegistry _entityRegistry;
  private final EntitySearchService _entitySearchService;
  private final SearchRanker _searchRanker;
  private final CacheManager _cacheManager;
  private final NonEmptyEntitiesCache _nonEmptyEntitiesCache;

  private final EntitySearchServiceCache _entitySearchServiceCache;

  private static final List<String> FILTER_RANKING =
      ImmutableList.of("entity", "typeNames", "platform", "origin", "tags", "glossaryTerms");

  public AllEntitiesSearchAggregator(EntityRegistry entityRegistry, EntitySearchService entitySearchService,
      SearchRanker searchRanker, CacheManager cacheManager, int batchSize) {
    _entityRegistry = entityRegistry;
    _entitySearchService = entitySearchService;
    _searchRanker = searchRanker;
    _cacheManager = cacheManager;
    _nonEmptyEntitiesCache = new NonEmptyEntitiesCache(entityRegistry, entitySearchService, cacheManager);
    _entitySearchServiceCache = new EntitySearchServiceCache(cacheManager, entitySearchService, batchSize);
  }

  @Nonnull
  @WithSpan
  public SearchResult search(@Nonnull List<String> entities, @Nonnull String input, @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion, int queryFrom, int querySize) {
    log.info(String.format(
        "Searching Search documents across entities: %s, input: %s, postFilters: %s, sortCriterion: %s, from: %s, size: %s",
        entities, input, postFilters, sortCriterion, queryFrom, querySize));

    // 1. Get entities to query for (Do not query entities without a single document)
    List<String> nonEmptyEntities;
    List<String> lowercaseEntities = entities.stream().map(String::toLowerCase).collect(Collectors.toList());
    try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "getNonEmptyEntities").time()) {
      nonEmptyEntities = _nonEmptyEntitiesCache.getNonEmptyEntities();
    }
    if (!entities.isEmpty()) {
      nonEmptyEntities = nonEmptyEntities.stream().filter(lowercaseEntities::contains).collect(Collectors.toList());
    }

    // 2. Get search results for each entity
    Map<String, SearchResult> searchResults =
        getSearchResultsForEachEntity(nonEmptyEntities, input, postFilters, sortCriterion, queryFrom, querySize);

    if (searchResults.isEmpty()) {
      return getEmptySearchResult(queryFrom, querySize);
    }

    Timer.Context postProcessTimer = MetricUtils.timer(this.getClass(), "postProcessTimer").time();

    // 3. Combine search results from all entities
    int numEntities = 0;
    List<SearchEntity> matchedResults = new ArrayList<>();
    Map<String, AggregationMetadata> aggregations = new HashMap<>();

    Map<String, Long> numResultsPerEntity = searchResults.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getNumEntities().longValue()));
    aggregations.put("entity", new AggregationMetadata().setName("entity")
        .setDisplayName("Type")
        .setAggregations(new LongMap(numResultsPerEntity))
        .setFilterValues(new FilterValueArray(SearchUtil.convertToFilters(numResultsPerEntity))));

    for (String entity : searchResults.keySet()) {
      SearchResult result = searchResults.get(entity);
      numEntities += result.getNumEntities();
      matchedResults.addAll(result.getEntities());
      // Merge filters
      result.getMetadata().getAggregations().forEach(metadata -> {
        if (aggregations.containsKey(metadata.getName())) {
          aggregations.put(metadata.getName(), merge(aggregations.get(metadata.getName()), metadata));
        } else {
          aggregations.put(metadata.getName(), metadata);
        }
      });
    }

    // 4. Rank results across entities
    List<SearchEntity> rankedResult = _searchRanker.rank(matchedResults);
    SearchResultMetadata finalMetadata =
        new SearchResultMetadata().setAggregations(new AggregationMetadataArray(rankFilterGroups(aggregations)));

    postProcessTimer.stop();
    return new SearchResult().setEntities(new SearchEntityArray(rankedResult))
        .setNumEntities(numEntities)
        .setFrom(queryFrom)
        .setPageSize(querySize)
        .setMetadata(finalMetadata);
  }

  private SearchResult getEmptySearchResult(int from, int size) {
    return new SearchResult().setEntities(new SearchEntityArray())
        .setNumEntities(0)
        .setFrom(from)
        .setPageSize(size)
        .setMetadata(new SearchResultMetadata().setAggregations(new AggregationMetadataArray()));
  }

  @WithSpan
  private Map<String, SearchResult> getSearchResultsForEachEntity(@Nonnull List<String> entities, @Nonnull String input,
      @Nullable Filter postFilters, @Nullable SortCriterion sortCriterion, int queryFrom, int querySize) {
    Map<String, SearchResult> searchResults;
    // Query the entity search service for all entities asynchronously
    try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "searchEntities").time()) {
      searchResults = ConcurrencyUtils.transformAndCollectAsync(entities, entity -> new Pair<>(entity,
          _entitySearchServiceCache.getSearcher(entity, input, postFilters, sortCriterion)
              .getSearchResults(queryFrom, querySize)))
          .stream()
          .filter(pair -> pair.getValue().getNumEntities() > 0)
          .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }
    return searchResults;
  }

  @SneakyThrows
  @VisibleForTesting
  static AggregationMetadata merge(AggregationMetadata one, AggregationMetadata two) {
    Map<String, Long> mergedMap =
        Stream.concat(one.getAggregations().entrySet().stream(), two.getAggregations().entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::sum));
    return one.clone()
        .setAggregations(new LongMap(mergedMap))
        .setFilterValues(new FilterValueArray(SearchUtil.convertToFilters(mergedMap)));
  }

  private List<AggregationMetadata> rankFilterGroups(Map<String, AggregationMetadata> aggregations) {
    Set<String> filterGroups = new HashSet<>(aggregations.keySet());
    List<AggregationMetadata> finalAggregations = new ArrayList<>(aggregations.size());
    for (String filterName : FILTER_RANKING) {
      if (filterGroups.contains(filterName)) {
        filterGroups.remove(filterName);
        finalAggregations.add(aggregations.get(filterName));
      }
    }
    filterGroups.forEach(filterName -> finalAggregations.add(aggregations.get(filterName)));
    return finalAggregations;
  }
}
