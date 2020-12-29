package graphql.resolvers;

import graphql.QueryContext;
import graphql.resolvers.exception.AuthorizationError;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;


/**
 * Abstract base class that should be extended by any resolver that serves data that should be behind an auth
 * wall. Simply checks whether the user is currently authenticated.
 *
 * In the future, we may choose to introduce additional "role-oriented" resolvers.
 */
public abstract class AuthenticatedResolver<T> implements DataFetcher<T> {

    /**
     * Abstract method to be overridden by subclasses to implement a secured resolver.
     */
    protected abstract T authenticatedGet(DataFetchingEnvironment environment) throws Exception;

    @Override
    final public T get(DataFetchingEnvironment environment) throws Exception {
        final QueryContext context = environment.getContext();
        if (context.isAuthenticated()) {
            return authenticatedGet(environment);
        }
        throw new AuthorizationError("Failed to authorize the current user.");
    }
}
