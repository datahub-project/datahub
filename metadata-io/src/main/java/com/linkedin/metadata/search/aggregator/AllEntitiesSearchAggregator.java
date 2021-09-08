package com.linkedin.metadata.search.aggregator;

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.LongMap;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.MatchMetadata;
import com.linkedin.metadata.search.MatchMetadataArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.utils.SearchUtil;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.util.Pair;
import io.opentelemetry.extension.annotations.WithSpan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class AllEntitiesSearchAggregator {
  private final EntityRegistry _entityRegistry;
  private final ESSearchDAO _esSearchDAO;
  private final Map<String, Set<String>> _filtersPerEntity;
  private final Map<String, String> _filtersToDisplayName;

  private static final List<String> FILTER_RANKING =
      ImmutableList.of("entity", "platform", "origin", "tags", "glossaryTerms");

  public AllEntitiesSearchAggregator(EntityRegistry entityRegistry, ESSearchDAO esSearchDAO) {
    _entityRegistry = entityRegistry;
    _esSearchDAO = esSearchDAO;
    _filtersPerEntity = getFiltersPerEntity(entityRegistry);
    _filtersToDisplayName = getFilterToDisplayName(entityRegistry);
  }

  private static Map<String, Set<String>> getFiltersPerEntity(EntityRegistry entityRegistry) {
    return entityRegistry.getEntitySpecs()
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue()
            .getSearchableFieldSpecs()
            .stream()
            .filter(spec -> spec.getSearchableAnnotation().isAddToFilters())
            .map(spec -> spec.getSearchableAnnotation().getFieldName())
            .collect(Collectors.toSet())));
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
              _esSearchDAO.search(entity, input, postFilters, sortCriterion, 0, from + size)))
          .filter(pair -> pair.getValue().getNumEntities() > 0)
          .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    if (searchResults.isEmpty()) {
      return getEmptySearchResult(from, size);
    }

    Timer.Context postProcessTimer = MetricUtils.timer(this.getClass(), "postProcessTimer").time();
    Set<String> commonFilters = getCommonFilters(searchResults.keySet());

    int numEntities = 0;
    List<IntermediateResult> matchedResults = new ArrayList<>();
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
      if (result.getMetadata().hasMatches()) {
        for (int i = 0; i < result.getEntities().size(); i++) {
          matchedResults.add(new IntermediateResult(result.getEntities().get(i),
              i < result.getMetadata().getMatches().size() ? result.getMetadata().getMatches().get(i)
                  : new MatchMetadata(), entity));
        }
      } else {
        result.getEntities()
            .forEach(urn -> matchedResults.add(new IntermediateResult(urn, new MatchMetadata(), entity)));
      }
      // Merge filters
      result.getMetadata()
          .getSearchResultMetadatas()
          .stream()
          .filter(metadata -> commonFilters.contains(metadata.getName()))
          .forEach(metadata -> {
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

    Pair<List<Urn>, List<MatchMetadata>> rankedResult = rankResults(matchedResults, numResultsPerEntity.entrySet()
        .stream()
        .sorted(Comparator.comparingLong(Map.Entry::getValue))
        .map(Map.Entry::getKey)
        .collect(Collectors.toList()));
    List<Urn> finalMatchedResults;
    List<MatchMetadata> finalMatchedMetadatas;
    if (matchedResults.size() <= from) {
      finalMatchedResults = Collections.emptyList();
      finalMatchedMetadatas = Collections.emptyList();
    } else {
      finalMatchedResults = rankedResult.getFirst().subList(from, Math.min(from + size, matchedResults.size()));
      finalMatchedMetadatas = rankedResult.getSecond().subList(from, Math.min(from + size, matchedResults.size()));
    }
    SearchResultMetadata finalMetadata = new SearchResultMetadata().setSearchResultMetadatas(
        new AggregationMetadataArray(rankFilterGroups(aggregations))).setUrns(new UrnArray(finalMatchedResults));
    finalMetadata.setMatches(new MatchMetadataArray(finalMatchedMetadatas));

    postProcessTimer.stop();
    return new SearchResult().setEntities(new UrnArray(finalMatchedResults))
        .setNumEntities(numEntities)
        .setFrom(from)
        .setPageSize(size)
        .setMetadata(finalMetadata);
  }

  private SearchResult getEmptySearchResult(int from, int size) {
    return new SearchResult().setEntities(new UrnArray())
        .setNumEntities(0)
        .setFrom(from)
        .setPageSize(size)
        .setMetadata(new SearchResultMetadata().setSearchResultMetadatas(new AggregationMetadataArray())
            .setUrns(new UrnArray())
            .setMatches(new MatchMetadataArray()));
  }

  private Set<String> getCommonFilters(Set<String> entities) {
    List<Set<String>> filtersPerEntity = entities.stream().map(_filtersPerEntity::get).collect(Collectors.toList());
    return filtersPerEntity.stream()
        .skip(1)
        .collect(() -> new HashSet<>(filtersPerEntity.get(0)), Set::retainAll, Set::retainAll);
  }

  @WithSpan
  private List<String> getNonEmptyEntities() {
    return _entityRegistry.getEntitySpecs()
        .keySet()
        .stream()
        .filter(entity -> _esSearchDAO.docCount(entity) > 0)
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

  private Pair<List<Urn>, List<MatchMetadata>> rankResults(List<IntermediateResult> intermediateResults,
      List<String> entityRanking) {
    Map<String, List<IntermediateResult>> resultsPerEntity =
        intermediateResults.stream().collect(Collectors.groupingBy(IntermediateResult::getEntityType));
    Map<String, Integer> indexPerEntity =
        resultsPerEntity.keySet().stream().collect(Collectors.toMap(Function.identity(), entity -> 0));
    Pair<List<Urn>, List<MatchMetadata>> result = new Pair<>(new ArrayList<>(), new ArrayList<>());
    while (indexPerEntity.size() > 0) {
      for (String entity : entityRanking) {
        if (!indexPerEntity.containsKey(entity)) {
          continue;
        }

        List<IntermediateResult> resultsForEntity = resultsPerEntity.get(entity);
        int index = indexPerEntity.get(entity);
        if (index < resultsForEntity.size()) {
          IntermediateResult chosenResult = resultsForEntity.get(index);
          result.getFirst().add(chosenResult.getUrn());
          result.getSecond().add(chosenResult.getMatchMetadata());
          if (index == resultsForEntity.size() - 1) {
            indexPerEntity.remove(entity);
          } else {
            indexPerEntity.put(entity, index + 1);
          }
        }
      }
    }
    return result;
  }

  @Value
  private static class IntermediateResult {
    Urn urn;
    MatchMetadata matchMetadata;
    String entityType;
  }
}
