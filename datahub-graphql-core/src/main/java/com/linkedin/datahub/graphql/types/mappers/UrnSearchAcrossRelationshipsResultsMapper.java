package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.data.template.DoubleMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.AggregationMetadata;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.FacetMetadata;
import com.linkedin.datahub.graphql.generated.MatchedField;
import com.linkedin.datahub.graphql.generated.SearchAcrossRelationshipsResult;
import com.linkedin.datahub.graphql.generated.SearchAcrossRelationshipsResults;
import com.linkedin.datahub.graphql.generated.SearchInsight;
import com.linkedin.datahub.graphql.resolvers.EntityTypeMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.util.SearchInsightsUtil;
import com.linkedin.metadata.search.RelationshipSearchEntity;
import com.linkedin.metadata.search.RelationshipSearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;


public class UrnSearchAcrossRelationshipsResultsMapper<T extends RecordTemplate, E extends Entity> {
  public static <T extends RecordTemplate, E extends Entity> SearchAcrossRelationshipsResults map(
      RelationshipSearchResult searchResult) {
    return new UrnSearchAcrossRelationshipsResultsMapper<T, E>().apply(searchResult);
  }

  public SearchAcrossRelationshipsResults apply(RelationshipSearchResult input) {
    final SearchAcrossRelationshipsResults result = new SearchAcrossRelationshipsResults();

    if (!input.hasFrom() || !input.hasPageSize() || !input.hasNumEntities()) {
      return result;
    }

    result.setStart(input.getFrom());
    result.setCount(input.getPageSize());
    result.setTotal(input.getNumEntities());

    final SearchResultMetadata searchResultMetadata = input.getMetadata();
    result.setSearchResults(input.getEntities().stream().map(this::mapResult).collect(Collectors.toList()));
    result.setFacets(searchResultMetadata.getAggregations().stream().map(this::mapFacet).collect(Collectors.toList()));

    return result;
  }

  private SearchAcrossRelationshipsResult mapResult(RelationshipSearchEntity searchEntity) {
    return new SearchAcrossRelationshipsResult(UrnToEntityMapper.map(searchEntity.getEntity()),
        getInsightsFromFeatures(searchEntity.getFeatures()), getMatchedFieldEntry(searchEntity.getMatchedFields()),
        searchEntity.getPath().stream().map(UrnToEntityMapper::map).collect(Collectors.toList()));
  }

  private FacetMetadata mapFacet(com.linkedin.metadata.search.AggregationMetadata aggregationMetadata) {
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

  private String convertFilterValue(String filterValue, boolean isEntityType) {
    if (isEntityType) {
      return EntityTypeMapper.getType(filterValue).toString();
    }
    return filterValue;
  }

  private List<SearchInsight> getInsightsFromFeatures(final DoubleMap features) {
    if (features == null) {
      return Collections.emptyList();
    }
    return SearchInsightsUtil.getInsightsFromFeatures(features);
  }

  private List<MatchedField> getMatchedFieldEntry(List<com.linkedin.metadata.search.MatchedField> highlightMetadata) {
    return highlightMetadata.stream()
        .map(field -> new MatchedField(field.getName(), field.getValue()))
        .collect(Collectors.toList());
  }
}
