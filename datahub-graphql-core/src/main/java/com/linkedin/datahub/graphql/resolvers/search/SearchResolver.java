package com.linkedin.datahub.graphql.resolvers.search;

import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.exception.ValidationException;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.SearchInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Resolver responsible for resolving the 'search' field of the Query type
 */
public class SearchResolver implements DataFetcher<CompletableFuture<SearchResults>> {

    private static final int DEFAULT_START = 0;
    private static final int DEFAULT_COUNT = 10;

    private final Map<EntityType, SearchableEntityType<?>> _typeToEntity;

    public SearchResolver(@Nonnull final List<SearchableEntityType<?>> searchableEntities) {
        _typeToEntity = searchableEntities.stream().collect(Collectors.toMap(
                SearchableEntityType::type,
                entity -> entity
        ));
    }

    @Override
    public CompletableFuture<SearchResults> get(DataFetchingEnvironment environment) {
        final SearchInput input = bindArgument(environment.getArgument("input"), SearchInput.class);

        // escape forward slash since it is a reserved character in Elasticsearch
        final String sanitizedQuery = ResolverUtils.escapeForwardSlash(input.getQuery());
        if (isBlank(sanitizedQuery)) {
            throw new ValidationException("'query' parameter cannot be null or empty");
        }

        final int start = input.getStart() != null ? input.getStart() : DEFAULT_START;
        final int count = input.getCount() != null ? input.getCount() : DEFAULT_COUNT;

        return CompletableFuture.supplyAsync(() -> {
            try {
                return _typeToEntity.get(input.getType()).search(
                        sanitizedQuery,
                        input.getFilters(),
                        start,
                        count,
                        environment.getContext()
                );
            } catch (Exception e) {
                throw new RuntimeException("Failed to execute search: "
                    + String.format("entity type %s, query %s, filters: %s, start: %s, count: %s",
                        input.getType(),
                        input.getQuery(),
                        input.getFilters(),
                        start,
                        count), e);
            }
        });
    }
}
