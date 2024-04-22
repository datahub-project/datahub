package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.metadata.search.SearchResultMetadata;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class UrnSearchResultsMapper<T extends RecordTemplate, E extends Entity> {
  public static <T extends RecordTemplate, E extends Entity> SearchResults map(
      @Nullable final QueryContext context,
      com.linkedin.metadata.search.SearchResult searchResult) {
    return new UrnSearchResultsMapper<T, E>().apply(context, searchResult);
  }

  public SearchResults apply(
      @Nullable final QueryContext context, com.linkedin.metadata.search.SearchResult input) {
    final SearchResults result = new SearchResults();

    if (!input.hasFrom() || !input.hasPageSize() || !input.hasNumEntities()) {
      return result;
    }

    result.setStart(input.getFrom());
    result.setCount(input.getPageSize());
    result.setTotal(input.getNumEntities());

    final SearchResultMetadata searchResultMetadata = input.getMetadata();
    result.setSearchResults(
        input.getEntities().stream()
            .map(r -> MapperUtils.mapResult(context, r))
            .collect(Collectors.toList()));
    result.setFacets(
        searchResultMetadata.getAggregations().stream()
            .map(f -> MapperUtils.mapFacet(context, f))
            .collect(Collectors.toList()));
    result.setSuggestions(
        searchResultMetadata.getSuggestions().stream()
            .map(MapperUtils::mapSearchSuggestion)
            .collect(Collectors.toList()));

    return result;
  }
}
