package com.linkedin.datahub.graphql.resolvers.search;

import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.exception.ValidationException;
import com.linkedin.datahub.graphql.generated.AutoCompleteInput;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Resolver responsible for resolving the 'autocomplete' field of the Query type
 */
public class AutoCompleteResolver implements DataFetcher<CompletableFuture<AutoCompleteResults>> {

    private static final int DEFAULT_LIMIT = 5;

    private final Map<EntityType, SearchableEntityType<?>> _typeToEntity;
    private final List<SearchableEntityType<?>> _searchableEntities;

    public AutoCompleteResolver(@Nonnull final List<SearchableEntityType<?>> searchableEntities) {
        _typeToEntity = searchableEntities.stream().collect(Collectors.toMap(
                SearchableEntityType::type,
                entity -> entity
        ));
        _searchableEntities = searchableEntities;
    }

    @Override
    public CompletableFuture<AutoCompleteResults> get(DataFetchingEnvironment environment) {
        final AutoCompleteInput input = bindArgument(environment.getArgument("input"), AutoCompleteInput.class);

        // escape forward slash since it is a reserved character in Elasticsearch
        final String sanitizedQuery = ResolverUtils.escapeForwardSlash(input.getQuery());
        if (isBlank(sanitizedQuery)) {
            throw new ValidationException("'query' parameter can not be null or empty");
        }

        final int limit = input.getLimit() != null ? input.getLimit() : DEFAULT_LIMIT;
        if (input.getType() != null) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return _typeToEntity.get(input.getType()).autoComplete(
                            sanitizedQuery,
                            input.getField(),
                            input.getFilters(),
                            limit,
                            environment.getContext()
                    );
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return new AutoCompleteResults(input.getQuery(), new ArrayList<String>());
            });
        } else {
            final CompletableFuture<AutoCompleteResults>[] autoCompletesFuture = _searchableEntities.stream().map(entity -> {
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        final AutoCompleteResults searchResult = entity.autoComplete(
                                sanitizedQuery,
                                input.getField(),
                                input.getFilters(),
                                limit,
                                environment.getContext()
                        );
                        return searchResult;
                    } catch (Exception e) {
                        return new AutoCompleteResults();
                    }
                });
            }).toArray(CompletableFuture[]::new);
            return CompletableFuture.allOf(autoCompletesFuture)
                .thenApplyAsync((res) -> {
                    AutoCompleteResults result = new AutoCompleteResults(input.getQuery(), new ArrayList<String>());
                    AutoCompleteResults[] autoCompleteResultsData = Arrays.stream(autoCompletesFuture).map((resultFuture -> {
                        AutoCompleteResults resultData = resultFuture.join();
                        if (resultData.getSuggestions() != null) {
                            result.setSuggestions(Stream.concat(result.getSuggestions().stream(), resultData.getSuggestions().stream()).collect(Collectors.toList()));
                        }
                        return resultData;
                    })).toArray(AutoCompleteResults[]::new);
                    return result;
                });
        }
    }
}
