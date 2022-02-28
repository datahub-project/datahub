package com.linkedin.metadata.search;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.DisjunctiveCriterion;
import com.linkedin.metadata.query.filter.DisjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.utils.FilterUtils;
import com.linkedin.metadata.search.utils.SearchUtils;
import io.opentelemetry.extension.annotations.WithSpan;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.cache.Cache;


@RequiredArgsConstructor
public class RelationshipSearchService {
  private final SearchService _searchService;
  private final GraphService _graphService;
  private final Cache cache;

  private static final String LEVEL_FILTER = "level";
  private static final String LEVEL_FILTER_INPUT = "level.keyword";
  private static final AggregationMetadata LEVEL_FILTER_GROUP = new AggregationMetadata().setName(LEVEL_FILTER)
      .setDisplayName("Level of Dependencies")
      .setFilterValues(new FilterValueArray(ImmutableList.of(new FilterValue().setValue("1").setFacetCount(0),
          new FilterValue().setValue("2").setFacetCount(0), new FilterValue().setValue("3+").setFacetCount(0))));
  private static final int MAX_RELATIONSHIPS = 1000000;
  private static final int MAX_TERMS = 50000;
  private static final SearchFlags SKIP_CACHE = new SearchFlags().setSkipCache(true);

  /**
   * Gets a list of documents that match given search request that is related to the input entity
   *
   * @param sourceUrn Urn of the source entity
   * @param direction Direction of the relationship
   * @param entities list of entities to search (If empty, searches across all entities)
   * @param input the search input text
   * @param inputFilters the request map with fields and values as filters to be applied to search hits
   * @param sortCriterion {@link SortCriterion} to be applied to search results
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @return a {@link RelationshipSearchResult} that contains a list of matched documents and related search result metadata
   */
  @Nonnull
  @WithSpan
  public RelationshipSearchResult searchAcrossRelationships(@Nonnull Urn sourceUrn, @Nonnull LineageDirection direction,
      @Nonnull List<String> entities, @Nullable String input, @Nullable Filter inputFilters,
      @Nullable SortCriterion sortCriterion, int from, int size) {
    // Cache multihop result for faster performance
    EntityLineageResult lineageResult = cache.get(Pair.of(sourceUrn, direction), EntityLineageResult.class);
    if (lineageResult == null) {
      lineageResult = _graphService.getLineage(sourceUrn, direction, 0, MAX_RELATIONSHIPS, 1000);
    }

    // Filter hopped result based on the set of entities to return and inputFilters before sending to search
    List<LineageRelationship> lineageRelationships =
        filterRelationships(lineageResult, new HashSet<>(entities), inputFilters);

    return getSearchResultInBatches(lineageRelationships, input != null ? input : "*", inputFilters, sortCriterion,
        from, size);
  }

  // Search service can only take up to 50K term filter, so query search service in batches
  private RelationshipSearchResult getSearchResultInBatches(List<LineageRelationship> lineageRelationships,
      @Nonnull String input, @Nullable Filter inputFilters, @Nullable SortCriterion sortCriterion, int from, int size) {
    RelationshipSearchResult finalResult =
        new RelationshipSearchResult().setEntities(new RelationshipSearchEntityArray(Collections.emptyList()))
            .setMetadata(new SearchResultMetadata().setAggregations(new AggregationMetadataArray()))
            .setFrom(from)
            .setPageSize(size)
            .setNumEntities(0);
    List<List<LineageRelationship>> batchedRelationships = Lists.partition(lineageRelationships, MAX_TERMS);
    int queryFrom = from;
    int querySize = size;
    for (List<LineageRelationship> batch : batchedRelationships) {
      List<String> entitiesToQuery = batch.stream()
          .map(relationship -> relationship.getEntity().getEntityType())
          .distinct()
          .collect(Collectors.toList());
      Map<Urn, LineageRelationship> urnToRelationship =
          lineageRelationships.stream().collect(Collectors.toMap(LineageRelationship::getEntity, Function.identity()));
      Filter finalFilter = buildFilter(urnToRelationship.keySet(), inputFilters);
      RelationshipSearchResult resultForBatch = buildRelationshipSearchResult(
          _searchService.searchAcrossEntities(entitiesToQuery, input, finalFilter, sortCriterion, queryFrom, querySize,
              SKIP_CACHE), urnToRelationship);
      queryFrom = Math.max(0, from - resultForBatch.getNumEntities());
      querySize = Math.max(0, size - resultForBatch.getEntities().size());
      finalResult = merge(finalResult, resultForBatch);
    }

    finalResult.getMetadata().getAggregations().add(0, LEVEL_FILTER_GROUP);
    return finalResult.setFrom(from).setPageSize(size);
  }

  @SneakyThrows
  public static RelationshipSearchResult merge(RelationshipSearchResult one, RelationshipSearchResult two) {
    RelationshipSearchResult finalResult = one.clone();
    finalResult.getEntities().addAll(two.getEntities());
    finalResult.setNumEntities(one.getNumEntities() + two.getNumEntities());

    Map<String, AggregationMetadata> aggregations = one.getMetadata()
        .getAggregations()
        .stream()
        .collect(Collectors.toMap(AggregationMetadata::getName, Function.identity()));
    two.getMetadata().getAggregations().forEach(metadata -> {
      if (aggregations.containsKey(metadata.getName())) {
        aggregations.put(metadata.getName(), SearchUtils.merge(aggregations.get(metadata.getName()), metadata));
      } else {
        aggregations.put(metadata.getName(), metadata);
      }
    });
    finalResult.getMetadata().setAggregations(new AggregationMetadataArray(FilterUtils.rankFilterGroups(aggregations)));
    return finalResult;
  }

  private Predicate<Integer> convertFilterToPredicate(List<String> levelFilterValues) {
    return levelFilterValues.stream().map(value -> {
      switch (value) {
        case "1":
          return (Predicate<Integer>) (Integer numHops) -> (numHops == 1);
        case "2":
          return (Predicate<Integer>) (Integer numHops) -> (numHops == 2);
        case "3+":
          return (Predicate<Integer>) (Integer numHops) -> (numHops > 2);
        default:
          throw new IllegalArgumentException(String.format("%s is not a valid filter value for level filters", value));
      }
    }).reduce(x -> false, Predicate::or);
  }

  private List<LineageRelationship> filterRelationships(@Nonnull EntityLineageResult lineageResult,
      @Nonnull Set<String> entities, @Nullable Filter inputFilters) {
    Stream<LineageRelationship> relationshipsFilteredByEntities = lineageResult.getRelationships().stream();
    if (!entities.isEmpty()) {
      relationshipsFilteredByEntities = relationshipsFilteredByEntities.filter(
          relationship -> entities.contains(relationship.getEntity().getEntityType()));
    }
    if (inputFilters != null && !CollectionUtils.isEmpty(inputFilters.getOr())) {
      ConjunctiveCriterion conjunctiveCriterion = inputFilters.getOr().get(0);
      if (conjunctiveCriterion.hasAnd()) {
        List<String> levelFilter = conjunctiveCriterion.getAnd()
            .stream()
            .filter(criterion -> criterion.getField().equals(LEVEL_FILTER_INPUT))
            .map(Criterion::getValue)
            .collect(Collectors.toList());
        if (!levelFilter.isEmpty()) {
          Predicate<Integer> levelPredicate = convertFilterToPredicate(levelFilter);
          return relationshipsFilteredByEntities.filter(relationship -> levelPredicate.test(relationship.getNumHops()))
              .collect(Collectors.toList());
        }
      }
    }
    return relationshipsFilteredByEntities.collect(Collectors.toList());
  }

  private Filter buildFilter(@Nonnull Set<Urn> urns, @Nullable Filter inputFilters) {
    Criterion urnMatchCriterion = new Criterion().setField("urn")
        .setValue("")
        .setValues(new StringArray(urns.stream().map(Object::toString).collect(Collectors.toList())));
    ConjunctiveCriterionArray urnFilter = new ConjunctiveCriterionArray(
        ImmutableList.of(new ConjunctiveCriterion().setAnd(new CriterionArray(ImmutableList.of(urnMatchCriterion)))));
    if (inputFilters == null) {
      return new Filter().setOr(urnFilter);
    }
    SearchUtils.validateFilter(inputFilters);
    Filter reducedFilters =
        SearchUtils.removeCriteria(inputFilters, criterion -> criterion.getField().equals(LEVEL_FILTER_INPUT));

    // If or filter is set, create a new filter that has two and clauses:
    // one with the original or filters and one with the urn filter
    if (reducedFilters.getOr() != null) {
      return new Filter().setAnd(new DisjunctiveCriterionArray(
          ImmutableList.of(new DisjunctiveCriterion().setOr(reducedFilters.getOr()),
              new DisjunctiveCriterion().setOr(urnFilter))));
    }
    if (reducedFilters.getAnd() != null) {
      // If and filter is set, append urn filter to the list of and filters
      DisjunctiveCriterionArray andFilters = new DisjunctiveCriterionArray(reducedFilters.getAnd());
      andFilters.add(new DisjunctiveCriterion().setOr(urnFilter));
      return new Filter().setAnd(andFilters);
    }
    return new Filter().setOr(urnFilter);
  }

  private RelationshipSearchResult buildRelationshipSearchResult(@Nonnull SearchResult searchResult,
      Map<Urn, LineageRelationship> urnToRelationship) {
    AggregationMetadataArray aggregations = new AggregationMetadataArray(searchResult.getMetadata().getAggregations());
    return new RelationshipSearchResult().setEntities(new RelationshipSearchEntityArray(searchResult.getEntities()
        .stream()
        .map(searchEntity -> buildRelationshipSearchEntity(searchEntity,
            urnToRelationship.get(searchEntity.getEntity())))
        .collect(Collectors.toList())))
        .setMetadata(new SearchResultMetadata().setAggregations(aggregations))
        .setFrom(searchResult.getFrom())
        .setPageSize(searchResult.getPageSize())
        .setNumEntities(searchResult.getNumEntities());
  }

  private RelationshipSearchEntity buildRelationshipSearchEntity(@Nonnull SearchEntity searchEntity,
      @Nullable LineageRelationship lineageRelationship) {
    RelationshipSearchEntity entity = new RelationshipSearchEntity(searchEntity.data());
    if (lineageRelationship != null) {
      entity.setPath(lineageRelationship.getPath());
      entity.setNumHops(lineageRelationship.getNumHops());
    }
    return entity;
  }
}
