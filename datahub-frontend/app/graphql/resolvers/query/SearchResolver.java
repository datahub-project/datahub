package graphql.resolvers.query;

import com.google.common.base.Strings;
import com.linkedin.datahub.dao.view.DocumentSearchDao;
import com.linkedin.r2.RemoteInvocationException;
import graphql.resolvers.AuthenticatedResolver;
import graphql.resolvers.exception.ValueValidationError;
import graphql.schema.DataFetchingEnvironment;
import play.Logger;
import utils.SearchUtil;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import static graphql.Constants.*;
import static graphql.resolvers.query.ResolverUtil.SEARCH_DAO_MAP;
import static graphql.resolvers.query.ResolverUtil.buildFacetFilters;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static utils.SearchUtil.*;
import static utils.SearchUtil._DEFAULT_PAGE_SIZE;

/**
 * Resolver responsible for resolving the 'dataset' field of Query
 */
public class SearchResolver extends AuthenticatedResolver<CompletableFuture<Map<String, Object>>> {

    @Override
    public CompletableFuture<Map<String, Object>> authenticatedGet(DataFetchingEnvironment environment) {

        /*
         * Extract Arguments
         */
        final Map<String, Object> input = environment.getArgument(INPUT_FIELD_NAME);
        final String entityType = (String) input.get("type");
        final String query = (String) input.get("query");
        final Integer start = (Integer) input.getOrDefault("start", _DEFAULT_START_VALUE);
        final Integer count = (Integer) input.getOrDefault("count", _DEFAULT_PAGE_SIZE);
        final List<Map<String, String>> facetFiltersInput = (List<Map<String, String>>) input.get("filters");

        // escape forward slash since it is a reserved character in Elasticsearch
        final String sanitizedQuery = SearchUtil.escapeForwardSlash(Strings.nullToEmpty(query));
        if (isBlank(sanitizedQuery)) {
            throw new ValueValidationError("'query' parameter can not be null or empty");
        }

        final Map<String, String> facetFilters = buildFacetFilters(facetFiltersInput, entityType);

        final DocumentSearchDao<?> documentSearchDao = SEARCH_DAO_MAP.get(entityType);
        if (documentSearchDao != null) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return documentSearchDao.graphQLSearch(sanitizedQuery, facetFilters, start, count);
                } catch (RemoteInvocationException e) {
                    Logger.error(
                            String.format("Failed to search documents. query: %s, start: %s, count: %s, facets: %s",
                                    query,
                                    start,
                                    count,
                                    facetFilters.toString()), e);
                    throw new RuntimeException("Failed to retrieve search results", e);
                }
            });
        } else {
            throw new ValueValidationError("Unrecognized entity type provided.");
        }
    }
}
