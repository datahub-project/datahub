package com.linkedin.datahub.graphql.resolvers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthenticationError;

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
        throw new AuthenticationError("Failed to authenticate the current user.");
    }
}
