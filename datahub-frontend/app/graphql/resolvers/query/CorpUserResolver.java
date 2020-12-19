package graphql.resolvers.query;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

import java.util.Map;

/**
 * Resolver responsible for resolving the 'corpUser' field of Query
 */
public class CorpUserResolver implements DataFetcher<Map<String, Object>> {

    @Override
    public Map<String, Object> get(DataFetchingEnvironment environment) throws Exception {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
