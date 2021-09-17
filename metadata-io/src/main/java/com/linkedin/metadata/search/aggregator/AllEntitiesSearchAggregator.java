package com.linkedin.metadata.search.aggregator;

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import com.linkedin.data.template.LongMap;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.ranker.SearchRanker;
import com.linkedin.metadata.search.utils.EntitySearchServiceCache;
import com.linkedin.metadata.utils.SearchUtil;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.util.Pair;
import io.opentelemetry.extension.annotations.WithSpan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.CacheManager;


@Slf4j
public class AllEntitiesSearchAggregator {
  private final EntityRegistry _entityRegistry;
  private final EntitySearchService _entitySearchService;
  private final SearchRanker _searchRanker;
  private final CacheManager _cacheManager;
  private final int _batchSize;

  private final EntitySearchServiceCache _entitySearchServiceCache;

  private final Map<String, String> _filtersToDisplayName;

  private static final List<String> FILTER_RANKING =
      ImmutableList.of("entity", "platform", "origin", "tags", "glossaryTerms");

  public AllEntitiesSearchAggregator(EntityRegistry entityRegistry, EntitySearchService entitySearchService,
      SearchRanker searchRanker, CacheManager cacheManager, int batchSize) {
    _entityRegistry = entityRegistry;
    _entitySearchService = entitySearchService;
    _searchRanker = searchRanker;
    _cacheManager = cacheManager;
    _batchSize = batchSize;
    _entitySearchServiceCache = new EntitySearchServiceCache(cacheManager, entitySearchService, batchSize);
    _filtersToDisplayName = getFilterToDisplayName(entityRegistry);
  }

  private static Map<String, String> getFilterToDisplayName(EntityRegistry entityRegistry) {
    return entityRegistry.getEntitySpecs()
        .values()
        .stream()
        .flatMap(spec -> spec.getSearchableFieldSpecs().stream())
        .filter(spec -> spec.getSearchableAnnotation().isAddToFilters())
        .collect(Collectors.toMap(spec -> spec.getSearchableAnnotation().getFieldName(),
            spec -> spec.getSearchableAnnotation().getFilterName(), (a, b) -> a));
  }

  @Nonnull
  @WithSpan
  public SearchResult search(@Nonnull List<String> entities, @Nonnull String input, @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion, int from, int size) {
    log.info(String.format(
        "Searching Search documents across entities: %s, input: %s, postFilters: %s, sortCriterion: %s, from: %s, size: %s",
        entities, input, postFilters, sortCriterion, from, size));
    List<String> nonEmptyEntities;
    List<String> lowercaseEntities = entities.stream().map(String::toLowerCase).collect(Collectors.toList());
    try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "getNonEmptyEntities").time()) {
      nonEmptyEntities = getNonEmptyEntities();
    }
    if (!entities.isEmpty()) {
      nonEmptyEntities = nonEmptyEntities.stream().filter(lowercaseEntities::contains).collect(Collectors.toList());
    }
    Map<String, SearchResult> searchResults;
    try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "searchEntities").time()) {
      searchResults = nonEmptyEntities.stream()
          .map(entity -> new Pair<>(entity,
              _entitySearchServiceCache.getSearcher(entity, input, postFilters, sortCriterion)
                  .getSearchResults(from, size)))
          .filter(pair -> pair.getValue().getNumEntities() > 0)
          .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    if (searchResults.isEmpty()) {
      return getEmptySearchResult(from, size);
    }

    Timer.Context postProcessTimer = MetricUtils.timer(this.getClass(), "postProcessTimer").time();

    int numEntities = 0;
    List<SearchEntity> matchedResults = new ArrayList<>();
    Map<String, AggregationMetadata> aggregations = new HashMap<>();

    Map<String, Long> numResultsPerEntity = searchResults.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getNumEntities().longValue()));
    aggregations.put("entity", new AggregationMetadata().setName("entity")
        .setDisplayName("Entity Type")
        .setAggregations(new LongMap(numResultsPerEntity))
        .setFilterValues(new FilterValueArray(SearchUtil.convertToFilters(numResultsPerEntity))));

    for (String entity : searchResults.keySet()) {
      SearchResult result = searchResults.get(entity);
      numEntities += result.getNumEntities();
      matchedResults.addAll(result.getEntities());
      // Merge filters
      result.getMetadata().getAggregations().forEach(metadata -> {
        if (aggregations.containsKey(metadata.getName())) {
          Map<String, Long> mergedMap =
              Stream.concat(aggregations.get(metadata.getName()).getAggregations().entrySet().stream(),
                  metadata.getAggregations().entrySet().stream())
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::sum));
          aggregations.put(metadata.getName(), new AggregationMetadata().setName(metadata.getName())
              .setDisplayName(_filtersToDisplayName.get(metadata.getName()))
              .setAggregations(new LongMap(mergedMap))
              .setFilterValues(new FilterValueArray(SearchUtil.convertToFilters(mergedMap))));
        } else {
          aggregations.put(metadata.getName(), metadata);
        }
      });
    }

    List<SearchEntity> rankedResult = _searchRanker.rank(matchedResults);
    List<SearchEntity> finalMatchedResults;
    if (matchedResults.size() <= from) {
      finalMatchedResults = Collections.emptyList();
    } else {
      finalMatchedResults = rankedResult.subList(from, Math.min(from + size, matchedResults.size()));
    }
    SearchResultMetadata finalMetadata =
        new SearchResultMetadata().setAggregations(new AggregationMetadataArray(rankFilterGroups(aggregations)));

    postProcessTimer.stop();
    return new SearchResult().setEntities(new SearchEntityArray(finalMatchedResults))
        .setNumEntities(numEntities)
        .setFrom(from)
        .setPageSize(size)
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
  private List<String> getNonEmptyEntities() {
    return _entityRegistry.getEntitySpecs()
        .keySet()
        .stream()
        .filter(entity -> _entitySearchService.docCount(entity) > 0)
        .collect(Collectors.toList());
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
