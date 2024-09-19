package com.linkedin.datahub.graphql.resolvers.search;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.AutoCompleteMultipleInput;
import com.linkedin.datahub.graphql.generated.AutoCompleteMultipleResults;
import com.linkedin.datahub.graphql.generated.AutoCompleteResultForEntity;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.view.DataHubViewInfo;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutocompleteUtils {
  private static final Logger _logger = LoggerFactory.getLogger(AutocompleteUtils.class.getName());

  private static final int DEFAULT_LIMIT = 5;

  private AutocompleteUtils() {}

  public static CompletableFuture<AutoCompleteMultipleResults> batchGetAutocompleteResults(
      List<SearchableEntityType<?, ?>> entities,
      String sanitizedQuery,
      AutoCompleteMultipleInput input,
      DataFetchingEnvironment environment,
      @Nullable DataHubViewInfo view) {
    final int limit = input.getLimit() != null ? input.getLimit() : DEFAULT_LIMIT;
    final QueryContext context = environment.getContext();

    final List<CompletableFuture<AutoCompleteResultForEntity>> autoCompletesFuture =
        entities.stream()
            .map(
                entity ->
                    GraphQLConcurrencyUtils.supplyAsync(
                        () -> {
                          final Filter filter =
                              ResolverUtils.buildFilter(input.getFilters(), input.getOrFilters());
                          final Filter finalFilter =
                              view != null
                                  ? SearchUtils.combineFilters(
                                      filter, view.getDefinition().getFilter())
                                  : filter;

                          try {
                            final AutoCompleteResults searchResult =
                                entity.autoComplete(
                                    sanitizedQuery,
                                    input.getField(),
                                    finalFilter,
                                    limit,
                                    environment.getContext());
                            return new AutoCompleteResultForEntity(
                                entity.type(),
                                searchResult.getSuggestions(),
                                searchResult.getEntities());
                          } catch (Exception e) {
                            _logger.error(
                                "Failed to execute autocomplete all: "
                                    + String.format(
                                        "field %s, query %s, filters: %s, limit: %s",
                                        input.getField(),
                                        input.getQuery(),
                                        filter,
                                        input.getLimit()),
                                e);
                            return new AutoCompleteResultForEntity(
                                entity.type(), Collections.emptyList(), Collections.emptyList());
                          }
                        },
                        AutocompleteUtils.class.getSimpleName(),
                        "batchGetAutocompleteResults"))
            .collect(Collectors.toList());
    return CompletableFuture.allOf(autoCompletesFuture.toArray(new CompletableFuture[0]))
        .thenApplyAsync(
            (res) -> {
              AutoCompleteMultipleResults result =
                  new AutoCompleteMultipleResults(sanitizedQuery, new ArrayList<>());
              List<AutoCompleteResultForEntity> suggestions =
                  autoCompletesFuture.stream()
                      .map(CompletableFuture::join)
                      .filter(
                          autoCompleteResultForEntity ->
                              autoCompleteResultForEntity.getSuggestions() != null
                                  && autoCompleteResultForEntity.getSuggestions().size() > 0)
                      .collect(Collectors.toList());
              result.setSuggestions(suggestions);
              return result;
            });
  }
}
