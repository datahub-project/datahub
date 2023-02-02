package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.datahub.graphql.generated.AggregationMetadata;
import com.linkedin.datahub.graphql.generated.FacetMetadata;
import com.linkedin.datahub.graphql.generated.MatchedField;
import com.linkedin.datahub.graphql.generated.SearchResult;
import com.linkedin.datahub.graphql.resolvers.EntityTypeMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.metadata.search.SearchEntity;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.linkedin.datahub.graphql.util.SearchInsightsUtil.*;


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
    boolean isEntityTypeFilter = aggregationMetadata.getName().equals("entity");
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

  public static String convertFilterValue(String filterValue, boolean isEntityType) {
    if (isEntityType) {
      return EntityTypeMapper.getType(filterValue).toString();
    }
    return filterValue;
  }

  public static List<MatchedField> getMatchedFieldEntry(List<com.linkedin.metadata.search.MatchedField> highlightMetadata) {
    return highlightMetadata.stream()
        .map(field -> new MatchedField(field.getName(), field.getValue()))
        .collect(Collectors.toList());
  }
}
