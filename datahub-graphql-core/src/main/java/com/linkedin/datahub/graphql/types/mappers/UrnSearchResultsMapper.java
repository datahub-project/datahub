package com.linkedin.datahub.graphql.types.mappers;

import com.google.common.collect.Streams;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.AggregationMetadata;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.FacetMetadata;
import com.linkedin.datahub.graphql.generated.MatchedField;
import com.linkedin.datahub.graphql.generated.SearchResult;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.metadata.search.MatchMetadata;
import com.linkedin.metadata.search.SearchResultMetadata;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class UrnSearchResultsMapper<T extends RecordTemplate, E extends Entity> {
  public static <T extends RecordTemplate, E extends Entity> SearchResults map(
      com.linkedin.metadata.search.SearchResult searchResult) {
    return new UrnSearchResultsMapper<T, E>().apply(searchResult);
  }

  public SearchResults apply(com.linkedin.metadata.search.SearchResult input) {
    final SearchResults result = new SearchResults();

    if (!input.hasFrom() || !input.hasPageSize() || !input.hasNumEntities()) {
      return result;
    }

    result.setStart(input.getFrom());
    result.setCount(input.getPageSize());
    result.setTotal(input.getNumEntities());

    final SearchResultMetadata searchResultMetadata = input.getMetadata();
    Stream<Entity> entities = input.getEntities().stream().map(urn -> UrnToEntityMapper.map(urn));
    if (searchResultMetadata.getMatches() != null) {
      result.setSearchResults(
          Streams.zip(entities, searchResultMetadata.getMatches().stream().map(this::getMatchedFieldEntry),
              SearchResult::new).collect(Collectors.toList()));
    } else {
      result.setSearchResults(
          entities.map(entity -> new SearchResult(entity, Collections.emptyList())).collect(Collectors.toList()));
    }
    result.setFacets(
        searchResultMetadata.getSearchResultMetadatas().stream().map(this::mapFacet).collect(Collectors.toList()));

    return result;
  }

  private FacetMetadata mapFacet(com.linkedin.metadata.search.AggregationMetadata aggregationMetadata) {
    final FacetMetadata facetMetadata = new FacetMetadata();
    facetMetadata.setField(aggregationMetadata.getName());
    facetMetadata.setAggregations(aggregationMetadata.getFilterValues()
        .stream()
        .map(filterValue -> new AggregationMetadata(filterValue.getValue(), filterValue.getFacetCount(),
            filterValue.getEntity() == null ? null : UrnToEntityMapper.map(filterValue.getEntity())))
        .collect(Collectors.toList()));
    return facetMetadata;
  }

  private List<MatchedField> getMatchedFieldEntry(MatchMetadata highlightMetadata) {
    return highlightMetadata.getMatchedFields()
        .stream()
        .map(field -> new MatchedField(field.getName(), field.getValue()))
        .collect(Collectors.toList());
  }
}
