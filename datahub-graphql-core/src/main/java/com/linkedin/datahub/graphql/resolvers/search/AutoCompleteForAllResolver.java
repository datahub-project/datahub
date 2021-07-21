package com.linkedin.datahub.graphql.resolvers.search;

import com.linkedin.datahub.graphql.exception.ValidationException;
import com.linkedin.datahub.graphql.generated.AutoCompleteInput;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.AutoCompleteAllResults;
import com.linkedin.datahub.graphql.generated.AutoCompleteResultForEntity;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Resolver responsible for resolving the 'autocomplete' field of the Query type
 */
public class AutoCompleteForAllResolver implements DataFetcher<CompletableFuture<AutoCompleteAllResults>> {

    private static final int DEFAULT_LIMIT = 5;
    private static final Logger _logger = LoggerFactory.getLogger(AutoCompleteForAllResolver.class.getName());

    private final List<SearchableEntityType<?>> _searchableEntities;

    public AutoCompleteForAllResolver(@Nonnull final List<SearchableEntityType<?>> searchableEntities) {
        _searchableEntities = searchableEntities;
    }

    @Override
    public CompletableFuture<AutoCompleteAllResults> get(DataFetchingEnvironment environment) {
        final AutoCompleteInput input = bindArgument(environment.getArgument("input"), AutoCompleteInput.class);

        // escape forward slash since it is a reserved character in Elasticsearch
        final String sanitizedQuery = ResolverUtils.escapeForwardSlash(input.getQuery());
        if (isBlank(sanitizedQuery)) {
            _logger.error("'query' parameter was null or empty");
            throw new ValidationException("'query' parameter can not be null or empty");
        }

        final int limit = input.getLimit() != null ? input.getLimit() : DEFAULT_LIMIT;
        final CompletableFuture<AutoCompleteResultForEntity>[] autoCompletesFuture = _searchableEntities.stream().map(entity -> {
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
                AutoCompleteAllResults result = new AutoCompleteAllResults(sanitizedQuery, new ArrayList<>());
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
