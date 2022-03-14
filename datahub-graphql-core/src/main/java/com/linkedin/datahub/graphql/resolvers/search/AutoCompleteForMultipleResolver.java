package com.linkedin.datahub.graphql.resolvers.search;

import com.linkedin.datahub.graphql.exception.ValidationException;
import com.linkedin.datahub.graphql.generated.AutoCompleteMultipleInput;
import com.linkedin.datahub.graphql.generated.AutoCompleteMultipleResults;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.*;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Resolver responsible for resolving the 'autocomplete' field of the Query type
 */
public class AutoCompleteForMultipleResolver implements DataFetcher<CompletableFuture<AutoCompleteMultipleResults>> {

    private static final Logger _logger = LoggerFactory.getLogger(AutoCompleteForMultipleResolver.class.getName());

    private final Map<EntityType, SearchableEntityType<?>> _typeToEntity;

    public AutoCompleteForMultipleResolver(@Nonnull final List<SearchableEntityType<?>> searchableEntities) {
        _typeToEntity = searchableEntities.stream().collect(Collectors.toMap(
            SearchableEntityType::type,
            entity -> entity
        ));
    }

    @Override
    public CompletableFuture<AutoCompleteMultipleResults> get(DataFetchingEnvironment environment) {
        final AutoCompleteMultipleInput input = bindArgument(environment.getArgument("input"), AutoCompleteMultipleInput.class);

        // escape forward slash since it is a reserved character in Elasticsearch
        final String sanitizedQuery = ResolverUtils.escapeForwardSlash(input.getQuery());
        if (isBlank(sanitizedQuery)) {
            _logger.error("'query' parameter was null or empty");
            throw new ValidationException("'query' parameter can not be null or empty");
        }

        List<EntityType> types = input.getTypes();
        if (types != null && types.size() > 0) {
            return AutocompleteUtils.batchGetAutocompleteResults(
                types.stream().map(_typeToEntity::get).collect(Collectors.toList()),
                sanitizedQuery,
                input,
                environment);
        }

        // By default, autocomplete only against the set of Searchable Entity Types.
        return AutocompleteUtils.batchGetAutocompleteResults(
            AUTO_COMPLETE_ENTITY_TYPES.stream().map(_typeToEntity::get).collect(Collectors.toList()),
            sanitizedQuery,
            input,
            environment);
    }
}
