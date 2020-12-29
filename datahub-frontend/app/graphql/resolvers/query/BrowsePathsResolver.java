package graphql.resolvers.query;

import com.linkedin.datahub.dao.view.BrowseDAO;
import com.linkedin.r2.RemoteInvocationException;
import graphql.resolvers.AuthenticatedResolver;
import graphql.resolvers.exception.ValueValidationError;
import graphql.schema.DataFetchingEnvironment;
import play.Logger;

import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static graphql.Constants.INPUT_FIELD_NAME;
import static graphql.resolvers.query.ResolverUtil.BROWSE_DAO_MAP;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Resolver responsible for resolving the 'browsePaths' field of Query
 */
public class BrowsePathsResolver extends AuthenticatedResolver<CompletableFuture<List<String>>> {
    @Override
    public CompletableFuture<List<String>> authenticatedGet(DataFetchingEnvironment environment) {

        final Map<String, Object> input = environment.getArgument(INPUT_FIELD_NAME);
        final String entityType = (String) input.get("type");
        final String urn = (String) input.get("urn");

        if (isBlank(urn)) {
            throw new ValueValidationError("'urn' argument cannot be null");
        }

        BrowseDAO<?> browseDAO = BROWSE_DAO_MAP.get(entityType);
        if (browseDAO != null) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return browseDAO.graphQLGetBrowsePaths(urn);
                } catch (RemoteInvocationException | URISyntaxException e) {
                    Logger.error("Failed to fetch browse paths for urn" + urn, e);
                    throw new RuntimeException("Failed to retrieve browse paths for urn" + urn, e);
                }
            });
        } else {
            throw new ValueValidationError("Unrecognized entity type provided.");
        }
    }
}