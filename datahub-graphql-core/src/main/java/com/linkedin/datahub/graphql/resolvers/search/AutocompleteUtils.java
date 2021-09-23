package com.linkedin.datahub.graphql.resolvers.search;

import com.linkedin.datahub.graphql.generated.AutoCompleteMultipleInput;
import com.linkedin.datahub.graphql.generated.AutoCompleteMultipleResults;
import com.linkedin.datahub.graphql.generated.AutoCompleteResultForEntity;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AutocompleteUtils {
  private static final Logger _logger = LoggerFactory.getLogger(AutocompleteUtils.class.getName());

  private static final int DEFAULT_LIMIT = 5;

  private AutocompleteUtils() { }

  public static CompletableFuture<AutoCompleteMultipleResults> batchGetAutocompleteResults(
      List<SearchableEntityType<?>> entities,
      String sanitizedQuery,
      AutoCompleteMultipleInput input,
      DataFetchingEnvironment environment
  ) {
    final int limit = input.getLimit() != null ? input.getLimit() : DEFAULT_LIMIT;

    final CompletableFuture<AutoCompleteResultForEntity>[] autoCompletesFuture = entities.stream().map(entity -> {
      return CompletableFuture.supplyAsync(() -> {
        try {
          final AutoCompleteResults searchResult = entity.autoComplete(
              sanitizedQuery,
              input.getField(),
              input.getFilters(),
              limit,
              environment.getContext()
          );
          final AutoCompleteResultForEntity autoCompleteResultForEntity =
              new AutoCompleteResultForEntity(entity.type(), searchResult.getSuggestions());
          return autoCompleteResultForEntity;
        } catch (Exception e) {
          _logger.error("Failed to execute autocomplete all: "
              + String.format("field %s, query %s, filters: %s, limit: %s",
              input.getField(),
              input.getQuery(),
              input.getFilters(),
              input.getLimit()) + " "
              + e.getMessage());
          return new AutoCompleteResultForEntity(entity.type(), new ArrayList<>());
        }
      });
    }).toArray(CompletableFuture[]::new);
    return CompletableFuture.allOf(autoCompletesFuture)
        .thenApplyAsync((res) -> {
          AutoCompleteMultipleResults result = new AutoCompleteMultipleResults(sanitizedQuery, new ArrayList<>());
          result.setSuggestions(Arrays.stream(autoCompletesFuture)
              .map(CompletableFuture::join)
              .filter(
                  autoCompleteResultForEntity ->
                      autoCompleteResultForEntity.getSuggestions() != null && autoCompleteResultForEntity.getSuggestions().size() > 0
              )
              .collect(Collectors.toList()));
          return result;
        });
  }
}
