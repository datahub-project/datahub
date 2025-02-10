package com.linkedin.metadata.search;

import static com.linkedin.metadata.utils.SearchUtil.*;

import com.linkedin.data.template.LongMap;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.cache.EntityDocCountCache;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.search.ranker.SearchRanker;
import com.linkedin.metadata.utils.SearchUtil;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SearchService {
  private final CachingEntitySearchService _cachingEntitySearchService;
  private final EntityDocCountCache _entityDocCountCache;
  private final SearchRanker _searchRanker;

  public SearchService(
      EntityDocCountCache entityDocCountCache,
      CachingEntitySearchService cachingEntitySearchService,
      SearchRanker searchRanker) {
    _cachingEntitySearchService = cachingEntitySearchService;
    _searchRanker = searchRanker;
    _entityDocCountCache = entityDocCountCache;
  }

  public Map<String, Long> docCountPerEntity(
      @Nonnull OperationContext opContext, @Nonnull List<String> entityNames) {
    return docCountPerEntity(opContext, entityNames, null);
  }

  public Map<String, Long> docCountPerEntity(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nullable Filter filter) {
    return getEntitiesToSearch(opContext, entityNames, 0).stream()
        .collect(
            Collectors.toMap(
                Function.identity(),
                entityName ->
                    _entityDocCountCache
                        .getEntityDocCount(opContext, filter)
                        .getOrDefault(entityName.toLowerCase(), 0L)));
  }

  /**
   * Gets a list of documents that match given search request. The results are aggregated and
   * filters are applied to the search hits and not the aggregation results.
   *
   * @param entityNames names of the entities
   * @param input the search input text
   * @param postFilters the request map with fields and values as filters to be applied to search
   *     hits
   * @param sortCriteria list of {@link SortCriterion} to be applied to search results
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @return a {@link SearchResult} that contains a list of matched documents and related search
   *     result metadata
   */
  @Nonnull
  public SearchResult search(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter postFilters,
      List<SortCriterion> sortCriteria,
      int from,
      int size) {
    List<String> entitiesToSearch = getEntitiesToSearch(opContext, entityNames, size);
    if (entitiesToSearch.isEmpty()) {
      // Optimization: If the indices are all empty, return empty result
      return getEmptySearchResult(from, size);
    }
    SearchResult result =
        _cachingEntitySearchService.search(
            opContext, entitiesToSearch, input, postFilters, sortCriteria, from, size, null);

    try {
      return result
          .copy()
          .setEntities(new SearchEntityArray(_searchRanker.rank(result.getEntities())));
    } catch (Exception e) {
      log.error("Failed to rank: {}, exception - {}", result, e.toString());
      throw new RuntimeException("Failed to rank " + result.toString());
    }
  }

  @Nonnull
  public SearchResult searchAcrossEntities(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entities,
      @Nonnull String input,
      @Nullable Filter postFilters,
      List<SortCriterion> sortCriteria,
      int from,
      int size) {
    return searchAcrossEntities(
        opContext, entities, input, postFilters, sortCriteria, from, size, null);
  }

  /**
   * Gets a list of documents that match given search request across multiple entities. The results
   * are aggregated and filters are applied to the search hits and not the aggregation results.
   *
   * @param entities list of entities to search (If empty, searches across all entities)
   * @param input the search input text
   * @param postFilters the request map with fields and values as filters to be applied to search
   *     hits
   * @param sortCriteria list of {@link SortCriterion} to be applied to search results
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @param facets list of facets we want aggregations for
   * @return a {@link SearchResult} that contains a list of matched documents and related search
   *     result metadata
   */
  @Nonnull
  public SearchResult searchAcrossEntities(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entities,
      @Nonnull String input,
      @Nullable Filter postFilters,
      List<SortCriterion> sortCriteria,
      int from,
      int size,
      @Nullable List<String> facets) {
    log.debug(
        String.format(
            "Searching Search documents entities: %s, input: %s, postFilters: %s, sortCriterion: %s, from: %s, size: %s",
            entities, input, postFilters, sortCriteria, from, size));
    // DEPRECATED
    // This is the legacy version of `_entityType`-- it operates as a special case and does not
    // support ORs, Unions, etc.
    // We will still provide it for backwards compatibility but when sending filters to the backend
    // use the new
    // filter name `_entityType` that we provide above. This is just provided to prevent a breaking
    // change for old clients.
    boolean aggregateByLegacyEntityFacet = facets != null && facets.contains("entity");
    if (aggregateByLegacyEntityFacet) {
      facets = new ArrayList<>(facets);
      facets.add(INDEX_VIRTUAL_FIELD);
    }
    List<String> nonEmptyEntities = getEntitiesToSearch(opContext, entities, size);
    if (nonEmptyEntities.isEmpty()) {
      // Optimization: If the indices are all empty, return empty result
      return getEmptySearchResult(from, size);
    }
    SearchResult result =
        _cachingEntitySearchService.search(
            opContext, nonEmptyEntities, input, postFilters, sortCriteria, from, size, facets);
    if (facets == null || facets.contains("entity") || facets.contains("_entityType")) {
      Optional<AggregationMetadata> entityTypeAgg =
          result.getMetadata().getAggregations().stream()
              .filter(aggMeta -> aggMeta.getName().equals(INDEX_VIRTUAL_FIELD))
              .findFirst();
      if (entityTypeAgg.isPresent()) {
        LongMap numResultsPerEntity = entityTypeAgg.get().getAggregations();
        result
            .getMetadata()
            .getAggregations()
            .add(
                new AggregationMetadata()
                    .setName("entity")
                    .setDisplayName("Type")
                    .setAggregations(numResultsPerEntity)
                    .setFilterValues(
                        new FilterValueArray(
                            SearchUtil.convertToFilters(
                                numResultsPerEntity, Collections.emptySet()))));
      } else {
        // Should not happen due to the adding of the _entityType aggregation before, but if it
        // does, best-effort count of entity types
        // Will not include entity types that had 0 results
        Map<String, Long> numResultsPerEntity =
            result.getEntities().stream()
                .collect(
                    Collectors.groupingBy(
                        entity -> entity.getEntity().getEntityType(), Collectors.counting()));
        result
            .getMetadata()
            .getAggregations()
            .add(
                new AggregationMetadata()
                    .setName("entity")
                    .setDisplayName("Type")
                    .setAggregations(new LongMap(numResultsPerEntity))
                    .setFilterValues(
                        new FilterValueArray(
                            SearchUtil.convertToFilters(
                                numResultsPerEntity, Collections.emptySet()))));
      }
    }
    return result;
  }

  /**
   * If no entities are provided, fallback to the list of non-empty entities
   *
   * @param inputEntities the requested entities
   * @return some entities to search
   */
  public List<String> getEntitiesToSearch(
      @Nonnull OperationContext opContext, @Nonnull Collection<String> inputEntities, int size) {
    List<String> lowercaseEntities =
        inputEntities.stream().map(String::toLowerCase).collect(Collectors.toList());

    if (lowercaseEntities.isEmpty()) {
      return opContext.withSpan(
          "getNonEmptyEntities",
          () -> _entityDocCountCache.getNonEmptyEntities(opContext),
          MetricUtils.DROPWIZARD_NAME,
          MetricUtils.name(this.getClass(), "getNonEmptyEntities"));
    }

    return lowercaseEntities;
  }

  /**
   * Gets a list of documents that match given search request across multiple entities. The results
   * are aggregated and filters are applied to the search hits and not the aggregation results.
   *
   * @param entities list of entities to search (If empty, searches across all entities)
   * @param input the search input text
   * @param postFilters the request map with fields and values as filters to be applied to search
   *     hits
   * @param sortCriteria list of {@link SortCriterion} to be applied to search results
   * @param scrollId opaque scroll identifier for passing to search backend
   * @param size the number of search hits to return
   * @return a {@link ScrollResult} that contains a list of matched documents and related search
   *     result metadata
   */
  @Nonnull
  public ScrollResult scrollAcrossEntities(
      @Nonnull OperationContext opContext,
      @Nonnull Collection<String> entities,
      @Nonnull String input,
      @Nullable Filter postFilters,
      List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      int size) {
    log.debug(
        String.format(
            "Searching Search documents entities: %s, input: %s, postFilters: %s, sortCriteria: %s, from: %s, size: %s",
            entities, input, postFilters, sortCriteria, scrollId, size));
    List<String> entitiesToSearch = getEntitiesToSearch(opContext, entities, size);
    if (entitiesToSearch.isEmpty()) {
      // No indices with non-zero entries: skip querying and return empty result
      return getEmptyScrollResult(size);
    }
    return _cachingEntitySearchService.scroll(
        opContext, entitiesToSearch, input, postFilters, sortCriteria, scrollId, keepAlive, size);
  }

  private static SearchResult getEmptySearchResult(int from, int size) {
    return new SearchResult()
        .setEntities(new SearchEntityArray())
        .setNumEntities(0)
        .setFrom(from)
        .setPageSize(size)
        .setMetadata(new SearchResultMetadata().setAggregations(new AggregationMetadataArray()));
  }

  private static ScrollResult getEmptyScrollResult(int size) {
    return new ScrollResult()
        .setEntities(new SearchEntityArray())
        .setNumEntities(0)
        .setPageSize(size)
        .setMetadata(new SearchResultMetadata().setAggregations(new AggregationMetadataArray()));
  }
}
