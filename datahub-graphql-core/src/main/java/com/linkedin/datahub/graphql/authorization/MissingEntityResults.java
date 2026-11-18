package com.linkedin.datahub.graphql.authorization;

import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.datahub.graphql.generated.ScrollResults;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import java.util.List;

/**
 * What backends answer about an entity that doesn't exist, for resolvers that answer for an entity
 * the caller can't view (see {@link AuthorizationUtils#hidesFromCaller}).
 */
public final class MissingEntityResults {

  private MissingEntityResults() {}

  /** A search or filter whose criteria name the entity. */
  public static SearchResult searchResult(int from, int size) {
    return new SearchResult()
        .setFrom(from)
        .setPageSize(size)
        .setNumEntities(0)
        .setEntities(new SearchEntityArray())
        .setMetadata(new SearchResultMetadata().setAggregations(new AggregationMetadataArray()));
  }

  /** The entity's edges in the graph. */
  public static EntityRelationships relationships(int start) {
    return new EntityRelationships()
        .setStart(start)
        .setCount(0)
        .setTotal(0)
        .setRelationships(new EntityRelationshipArray());
  }

  /** A GraphQL search whose criteria name the entity. */
  public static SearchResults searchResults(int start) {
    final SearchResults results = new SearchResults();
    results.setStart(start);
    results.setCount(0);
    results.setTotal(0);
    results.setSearchResults(List.of());
    results.setFacets(List.of());
    return results;
  }

  /** A GraphQL scroll whose criteria name the entity. */
  public static ScrollResults scrollResults() {
    final ScrollResults results = new ScrollResults();
    results.setCount(0);
    results.setTotal(0);
    results.setSearchResults(List.of());
    results.setFacets(List.of());
    return results;
  }
}
