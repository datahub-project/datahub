package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.datahub.graphql.generated.AggregationMetadata;
import com.linkedin.datahub.graphql.generated.FacetMetadata;
import com.linkedin.datahub.graphql.generated.MatchedField;
import com.linkedin.datahub.graphql.generated.SearchResult;
import com.linkedin.datahub.graphql.resolvers.EntityTypeMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.metadata.search.SearchEntity;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.linkedin.datahub.graphql.util.SearchInsightsUtil.*;
import static com.linkedin.metadata.utils.SearchUtil.*;


public class MapperUtils {

  private MapperUtils() {

  }

  public static SearchResult mapResult(SearchEntity searchEntity) {
    return new SearchResult(UrnToEntityMapper.map(searchEntity.getEntity()),
        getInsightsFromFeatures(searchEntity.getFeatures()),
        getMatchedFieldEntry(searchEntity.getMatchedFields()));
  }

  public static FacetMetadata mapFacet(com.linkedin.metadata.search.AggregationMetadata aggregationMetadata) {
    final FacetMetadata facetMetadata = new FacetMetadata();
    List<String> aggregationFacets = List.of(aggregationMetadata.getName().split(AGGREGATION_SEPARATOR_CHAR));
    List<Boolean> isEntityTypeFilter = aggregationFacets.stream().map(
        facet -> facet.equals("entity") || facet.contains("_entityType")).collect(Collectors.toList());
    facetMetadata.setField(aggregationMetadata.getName());
    facetMetadata.setDisplayName(
        Optional.ofNullable(aggregationMetadata.getDisplayName()).orElse(aggregationMetadata.getName()));
    facetMetadata.setAggregations(aggregationMetadata.getFilterValues()
        .stream()
        .map(filterValue -> new AggregationMetadata(convertFilterValue(filterValue.getValue(), isEntityTypeFilter),
            filterValue.getFacetCount(),
            filterValue.getEntity() == null ? null : UrnToEntityMapper.map(filterValue.getEntity())))
        .collect(Collectors.toList()));
    return facetMetadata;
  }

  public static String convertFilterValue(String filterValue, List<Boolean> isEntityTypeFilter) {
    String[] aggregations = filterValue.split(AGGREGATION_SEPARATOR_CHAR);
    return IntStream.range(0, aggregations.length).mapToObj(
        idx -> idx < isEntityTypeFilter.size() && isEntityTypeFilter.get(idx) ? EntityTypeMapper.getType(aggregations[idx]).toString() : aggregations[idx])
        .collect(Collectors.joining(AGGREGATION_SEPARATOR_CHAR));
  }

  public static List<MatchedField> getMatchedFieldEntry(List<com.linkedin.metadata.search.MatchedField> highlightMetadata) {
    return highlightMetadata.stream()
        .map(field -> new MatchedField(field.getName(), field.getValue()))
        .collect(Collectors.toList());
  }
}
