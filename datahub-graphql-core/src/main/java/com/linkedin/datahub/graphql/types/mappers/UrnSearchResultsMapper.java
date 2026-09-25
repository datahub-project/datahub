package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.SearchResult;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResultMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
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
    // GraphQL declares SearchResult.entity as non-null. UrnToEntityMapper returns null for a URN
    // whose entity type it does not model, and a single null element then bubbles up through the
    // non-null [SearchResult!]! list and fails the entire query (e.g. one un-mappable hit nulls a
    // whole page of semanticSearchAcrossEntities). Drop such hits with a warning instead — one bad
    // hit should degrade the page by a row, not discard it. `total` stays the backend hit count,
    // consistent with authorization/lifecycle post-filtering already returning fewer rows.
    final List<SearchResult> mappedResults = new ArrayList<>();
    for (SearchEntity searchEntity : input.getEntities()) {
      final SearchResult mapped = MapperUtils.mapResult(context, searchEntity);
      if (mapped.getEntity() == null) {
        log.warn(
            "Search hit {} has no GraphQL entity mapping; excluding it from results rather than "
                + "failing the whole result set.",
            searchEntity.getEntity());
        continue;
      }
      mappedResults.add(mapped);
    }
    result.setSearchResults(mappedResults);
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
