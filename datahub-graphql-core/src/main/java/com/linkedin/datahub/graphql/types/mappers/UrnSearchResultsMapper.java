package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.metadata.search.SearchResultMetadata;
import java.util.stream.Collectors;

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
    result.setSearchResults(
        input.getEntities().stream().map(MapperUtils::mapResult).collect(Collectors.toList()));
    result.setFacets(
        searchResultMetadata.getAggregations().stream()
            .map(MapperUtils::mapFacet)
            .collect(Collectors.toList()));
    result.setSuggestions(
        searchResultMetadata.getSuggestions().stream()
            .map(MapperUtils::mapSearchSuggestion)
            .collect(Collectors.toList()));

    return result;
  }
}
