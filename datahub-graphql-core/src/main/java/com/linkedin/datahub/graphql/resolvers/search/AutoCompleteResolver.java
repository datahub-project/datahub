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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Resolver responsible for resolving the 'autocomplete' field of the Query type
 */
public class AutoCompleteResolver implements DataFetcher<CompletableFuture<AutoCompleteResults>> {

    private static final int DEFAULT_LIMIT = 5;

    private static final Logger _logger = LoggerFactory.getLogger(AutoCompleteResolver.class.getName());

    private final Map<EntityType, SearchableEntityType<?, ?>> _typeToEntity;

    public AutoCompleteResolver(@Nonnull final List<SearchableEntityType<?, ?>> searchableEntities) {
        _typeToEntity = searchableEntities.stream().collect(Collectors.toMap(
                SearchableEntityType::type,
                entity -> entity
        ));
    }

    @Override
    public CompletableFuture<AutoCompleteResults> get(DataFetchingEnvironment environment) {
        final AutoCompleteInput input = bindArgument(environment.getArgument("input"), AutoCompleteInput.class);

        // escape forward slash since it is a reserved character in Elasticsearch
        final String sanitizedQuery = ResolverUtils.escapeForwardSlash(input.getQuery());
        if (isBlank(sanitizedQuery)) {
            _logger.error("'query' parameter was null or empty");
            throw new ValidationException("'query' parameter can not be null or empty");
        }

        final int limit = input.getLimit() != null ? input.getLimit() : DEFAULT_LIMIT;
            return CompletableFuture.supplyAsync(() -> {
                try {
                    _logger.debug("Executing autocomplete. "
                        + String.format("entity type %s, field %s, query %s, filters: %s, limit: %s",
                        input.getType(),
                        input.getField(),
                        input.getQuery(),
                        input.getFilters(),
                        input.getLimit()));
                    return _typeToEntity.get(input.getType()).autoComplete(
                            sanitizedQuery,
                            input.getField(),
                            input.getFilters(),
                            limit,
                            environment.getContext()
                    );
                } catch (Exception e) {
                    _logger.error("Failed to execute autocomplete: "
                        + String.format("entity type %s, field %s, query %s, filters: %s, limit: %s",
                        input.getType(),
                        input.getField(),
                        input.getQuery(),
                        input.getFilters(),
                        input.getLimit()) + " "
                        + e.getMessage());
                    throw new RuntimeException("Failed to execute autocomplete: "
                            + String.format("entity type %s, field %s, query %s, filters: %s, limit: %s",
                            input.getType(),
                            input.getField(),
                            input.getQuery(),
                            input.getFilters(),
                            input.getLimit()), e);
                }
            });
    }
}
