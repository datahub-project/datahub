/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.ScrollResults;
import com.linkedin.metadata.search.SearchResultMetadata;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class UrnScrollResultsMapper<T extends RecordTemplate, E extends Entity> {
  public static <T extends RecordTemplate, E extends Entity> ScrollResults map(
      @Nullable final QueryContext context,
      com.linkedin.metadata.search.ScrollResult scrollResult) {
    return new UrnScrollResultsMapper<T, E>().apply(context, scrollResult);
  }

  public ScrollResults apply(
      @Nullable final QueryContext context, com.linkedin.metadata.search.ScrollResult input) {
    final ScrollResults result = new ScrollResults();

    if (!input.hasScrollId() && (!input.hasPageSize() || !input.hasNumEntities())) {
      return result;
    }

    result.setNextScrollId(input.getScrollId());
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

    return result;
  }
}
