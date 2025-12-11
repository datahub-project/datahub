/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.metadata.query.AutoCompleteResult;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class AutoCompleteResultsMapper
    implements ModelMapper<AutoCompleteResult, AutoCompleteResults> {

  public static final AutoCompleteResultsMapper INSTANCE = new AutoCompleteResultsMapper();

  public static AutoCompleteResults map(
      @Nullable final QueryContext context, @Nonnull final AutoCompleteResult results) {
    return INSTANCE.apply(context, results);
  }

  @Override
  public AutoCompleteResults apply(
      @Nullable final QueryContext context, @Nonnull final AutoCompleteResult input) {
    final AutoCompleteResults result = new AutoCompleteResults();
    result.setQuery(input.getQuery());
    result.setSuggestions(input.getSuggestions());
    result.setEntities(
        input.getEntities().stream()
            .map(entity -> UrnToEntityMapper.map(context, entity.getUrn()))
            .collect(Collectors.toList()));
    return result;
  }
}
