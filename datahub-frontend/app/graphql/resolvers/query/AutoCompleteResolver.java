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

import static graphql.Constants.INPUT_FIELD_NAME;
import static graphql.resolvers.query.ResolverUtil.SEARCH_DAO_MAP;
import static graphql.resolvers.query.ResolverUtil.buildFacetFilters;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static utils.SearchUtil.*;

public class AutoCompleteResolver extends AuthenticatedResolver<CompletableFuture<Map<String, Object>>> {

    @Override
    public CompletableFuture<Map<String, Object>> authenticatedGet(DataFetchingEnvironment environment) {

        /*
         * Extract Arguments
         */
        final Map<String, Object> input = environment.getArgument(INPUT_FIELD_NAME);
        final String entityType = (String) input.get("type");
        final String query = (String) input.get("query");
        final String field = (String) input.get("field");
        final Integer limit =  (Integer) input.getOrDefault("limit", _DEFAULT_LIMIT_VALUE);
        final List<Map<String, String>> facetFiltersInput = (List<Map<String, String>>) input.get("filters");

        if (isBlank(field)) {
            throw new ValueValidationError("'field' parameter can not be null or empty");
        }

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
                    return documentSearchDao.graphQLAutoComplete(sanitizedQuery, field, facetFilters, limit);
                } catch (RemoteInvocationException e) {
                    Logger.error(
                            String.format("Failed to autocomplete. query: %s, facets: %s, limit: %s",
                                    query,
                                    facetFilters.toString(),
                                    limit));
                    throw new RuntimeException("Failed to retrieve autocomplete results");
                }
            });
        } else {
            throw new ValueValidationError("Unrecognized entity type provided.");
        }
    }
}