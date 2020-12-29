package graphql.resolvers.query;

import graphql.resolvers.AuthenticatedResolver;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

import java.util.Map;

/**
 * Resolver responsible for resolving the 'corpUser' field of Query
 */
public class CorpUserResolver extends AuthenticatedResolver<Map<String, Object>> {
    @Override
    public Map<String, Object> authenticatedGet(DataFetchingEnvironment environment) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
