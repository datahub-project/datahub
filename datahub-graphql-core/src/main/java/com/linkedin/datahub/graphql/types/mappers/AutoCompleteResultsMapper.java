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
