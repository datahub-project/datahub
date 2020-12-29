package graphql.resolvers.query;

import com.linkedin.datahub.dao.view.BrowseDAO;
import com.linkedin.r2.RemoteInvocationException;
import graphql.resolvers.AuthenticatedResolver;
import graphql.resolvers.exception.ValueValidationError;
import graphql.schema.DataFetchingEnvironment;
import play.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static graphql.Constants.INPUT_FIELD_NAME;
import static graphql.resolvers.query.ResolverUtil.BROWSE_DAO_MAP;
import static graphql.resolvers.query.ResolverUtil.buildFacetFilters;
import static utils.SearchUtil._DEFAULT_PAGE_SIZE;
import static utils.SearchUtil._DEFAULT_START_VALUE;

/**
 * Resolver responsible for resolving the 'browse' field of Query
 */
public class BrowseResolver extends AuthenticatedResolver<CompletableFuture<Map<String, Object>>> {
    @Override
    public CompletableFuture<Map<String, Object>> authenticatedGet(DataFetchingEnvironment environment) {

        final Map<String, Object> input = environment.getArgument(INPUT_FIELD_NAME);
        final String entityType = (String) input.get("type");
        final String path = (String) input.getOrDefault("path", "");
        final Integer start = (Integer) input.getOrDefault("start", _DEFAULT_START_VALUE);
        final Integer count = (Integer) input.getOrDefault("count", _DEFAULT_PAGE_SIZE);
        final List<Map<String, String>> facetFiltersInput = (List<Map<String, String>>) input.get("filters");

        Map<String, String> facetFilters = buildFacetFilters(facetFiltersInput, entityType);
        BrowseDAO<?> browseDAO = BROWSE_DAO_MAP.get(entityType);
        if (browseDAO != null) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return browseDAO.graphQLBrowse(path, facetFilters, start, count);
                } catch (RemoteInvocationException e) {
                    Logger.error(
                            String.format("Failed to browse. path: %s, start: %s, count: %s, facets: %s",
                                    path,
                                    start,
                                    count,
                                    facetFilters.toString()), e);
                    throw new RuntimeException("Failed to retrieve browse results", e);
                }
            });
        } else {
            throw new ValueValidationError("Unrecognized entity type provided.");
        }
    }
}

