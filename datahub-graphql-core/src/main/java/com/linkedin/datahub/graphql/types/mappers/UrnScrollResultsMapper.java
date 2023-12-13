package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.ScrollResults;
import com.linkedin.metadata.search.SearchResultMetadata;
import java.util.stream.Collectors;

public class UrnScrollResultsMapper<T extends RecordTemplate, E extends Entity> {
  public static <T extends RecordTemplate, E extends Entity> ScrollResults map(
      com.linkedin.metadata.search.ScrollResult scrollResult) {
    return new UrnScrollResultsMapper<T, E>().apply(scrollResult);
  }

  public ScrollResults apply(com.linkedin.metadata.search.ScrollResult input) {
    final ScrollResults result = new ScrollResults();

    if (!input.hasScrollId() && (!input.hasPageSize() || !input.hasNumEntities())) {
      return result;
    }

    result.setNextScrollId(input.getScrollId());
    result.setCount(input.getPageSize());
    result.setTotal(input.getNumEntities());

    final SearchResultMetadata searchResultMetadata = input.getMetadata();
    result.setSearchResults(
        input.getEntities().stream().map(MapperUtils::mapResult).collect(Collectors.toList()));
    result.setFacets(
        searchResultMetadata.getAggregations().stream()
            .map(MapperUtils::mapFacet)
            .collect(Collectors.toList()));

    return result;
  }
}
