package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.SearchResult;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.types.common.mappers.util.KnownEntities;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.utils.UnknownDataGuard;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class UrnSearchResultsMapper<T extends RecordTemplate, E extends Entity> {
  private static final UnknownDataGuard UNREPRESENTABLE =
      UnknownDataGuard.forSite(UrnSearchResultsMapper.class, "search hit");

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
    result.setSearchResults(mapKnownResults(context, input.getEntities()));
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

  /**
   * Maps search hits to results. GraphQL declares SearchResult.entity as non-null, and a single
   * null element bubbles up through the non-null [SearchResult!]! list and fails the entire query.
   * So hits GraphQL can't represent are left out, one row each, instead: entity types it doesn't
   * model, and entity types the registry doesn't know, including inside the urn's key (e.g. a
   * monitor on an entity type a newer version added, read after a rollback). Totals stay the
   * backend hit count, as with authorization post-filtering.
   */
  static List<SearchResult> mapKnownResults(
      @Nullable final QueryContext context, @Nonnull final List<SearchEntity> hits) {
    final List<SearchResult> mappedResults = new ArrayList<>(hits.size());
    for (SearchEntity hit : hits) {
      final boolean known = KnownEntities.isKnown(context, hit.getEntity());
      final SearchResult mapped = known ? MapperUtils.mapResult(context, hit) : null;
      if (mapped == null || mapped.getEntity() == null) {
        UNREPRESENTABLE.skippedBecause(
            KnownEntities.skipMetrics(context, known),
            hit.getEntity().getEntityType(),
            "GraphQL can't represent its entity type",
            hit.getEntity());
        continue;
      }
      mappedResults.add(mapped);
    }
    return mappedResults;
  }
}
