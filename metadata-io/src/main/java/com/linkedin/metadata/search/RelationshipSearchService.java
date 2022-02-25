package com.linkedin.metadata.search;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.DisjunctiveCriterion;
import com.linkedin.metadata.query.filter.DisjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.utils.SearchUtils;
import io.opentelemetry.extension.annotations.WithSpan;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import org.apache.commons.collections.CollectionUtils;


@RequiredArgsConstructor
public class RelationshipSearchService {
  private final SearchService _searchService;
  private final GraphService _graphService;

  private static final String LEVEL_FILTER = "level";
  private static final String LEVEL_FILTER_INPUT = "level.keyword";
  private static final AggregationMetadata LEVEL_FILTER_GROUP = new AggregationMetadata().setName(LEVEL_FILTER)
      .setDisplayName("Level of Dependencies")
      .setFilterValues(new FilterValueArray(ImmutableList.of(new FilterValue().setValue("1").setFacetCount(0),
          new FilterValue().setValue("2").setFacetCount(0), new FilterValue().setValue("3+").setFacetCount(0))));
  private static final int MAX_RELATIONSHIPS = 1000000;
  private static final int MAX_TERMS = 60000;

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
    EntityLineageResult lineageResult = _graphService.getLineage(sourceUrn, direction, 0, MAX_RELATIONSHIPS, 1000);
    List<LineageRelationship> lineageRelationships = filterRelationships(lineageResult, inputFilters);
    List<String> entitiesToQuery = lineageRelationships.stream()
        .map(relationship -> relationship.getEntity().getEntityType())
        .distinct()
        .filter(entities::contains)
        .collect(Collectors.toList());
    Map<Urn, LineageRelationship> urnToRelationship =
        lineageRelationships.stream().collect(Collectors.toMap(LineageRelationship::getEntity, Function.identity()));
    Filter finalFilter = buildFilter(urnToRelationship.keySet(), inputFilters);
    SearchResult searchResult =
        _searchService.searchAcrossEntities(entitiesToQuery, input != null ? input : "*", finalFilter, sortCriterion,
            from, size);
    return buildRelationshipSearchResult(searchResult, urnToRelationship);
  }

  private Predicate<Integer> convertFilterToPredicate(List<String> levelFilterValues) {
    return levelFilterValues.stream().map(value -> {
      switch (value) {
        case "1":
          return (Predicate<Integer>) (Integer pathLength1) -> (pathLength1 == 0);
        case "2":
          return (Predicate<Integer>) (Integer pathLength) -> (pathLength == 1);
        case "3+":
          return (Predicate<Integer>) (Integer pathLength) -> (pathLength > 1);
        default:
          throw new IllegalArgumentException(String.format("%s is not a valid filter value for level filters", value));
      }
    }).reduce(x -> false, Predicate::or);
  }

  private List<LineageRelationship> filterRelationships(@Nonnull EntityLineageResult lineageResult,
      @Nullable Filter inputFilters) {
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
          return lineageResult.getRelationships()
              .stream()
              .filter(relationship -> levelPredicate.test(relationship.getPath().size()))
              .limit(MAX_TERMS)
              .collect(Collectors.toList());
        }
      }
    }

    return lineageResult.getRelationships().subList(0, Math.min(lineageResult.getRelationships().size(), MAX_TERMS));
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
    aggregations.add(0, LEVEL_FILTER_GROUP);
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
    return new RelationshipSearchEntity(searchEntity.data()).setPath(
        Optional.ofNullable(lineageRelationship).map(LineageRelationship::getPath).orElse(new UrnArray()));
  }
}
