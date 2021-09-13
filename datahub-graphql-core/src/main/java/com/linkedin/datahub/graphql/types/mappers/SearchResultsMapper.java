package com.linkedin.datahub.graphql.types.mappers;

import com.google.common.collect.Streams;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.AggregationMetadata;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.FacetMetadata;
import com.linkedin.datahub.graphql.generated.MatchedField;
import com.linkedin.datahub.graphql.generated.SearchResult;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.metadata.query.MatchMetadata;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.restli.common.CollectionResponse;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import java.util.function.Function;
import java.util.stream.Collectors;


public class SearchResultsMapper<T extends RecordTemplate, E extends Entity> {

  public static <T extends RecordTemplate, E extends Entity> SearchResults map(
      @Nonnull final CollectionResponse<T> results, @Nonnull final Function<T, E> elementMapper) {
    return new SearchResultsMapper<T, E>().apply(results, elementMapper);
  }

  public SearchResults apply(@Nonnull final CollectionResponse<T> input, @Nonnull final Function<T, E> elementMapper) {
    final SearchResults result = new SearchResults();

    if (!input.hasPaging()) {
      throw new RuntimeException("Invalid search response received. Unable to find paging details.");
    }
    result.setStart(input.getPaging().getStart());
    result.setCount(input.getPaging().getCount());
    result.setTotal(input.getPaging().getTotal());

    final SearchResultMetadata searchResultMetadata = new SearchResultMetadata(input.getMetadataRaw());
    Stream<E> entities = input.getElements().stream().map(elementMapper::apply);
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

  private FacetMetadata mapFacet(com.linkedin.metadata.query.AggregationMetadata aggregationMetadata) {
    final FacetMetadata facetMetadata = new FacetMetadata();
    facetMetadata.setField(aggregationMetadata.getName());
    facetMetadata.setAggregations(aggregationMetadata.getAggregations()
        .entrySet()
        .stream()
        .map(entry -> new AggregationMetadata(entry.getKey(), entry.getValue()))
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
