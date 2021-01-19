package com.linkedin.datahub.graphql.resolvers;

import com.linkedin.datahub.graphql.exception.AuthorizationError;
import graphql.schema.DataFetchingEnvironment;

/**
 * Abstract base class that should be leveraged for any operation that should be protected by authorization.
 */
public abstract class AuthorizedResolver<T> extends AuthenticatedResolver<T> {

    /**
     * Abstract method to be overridden by subclasses to implement a authorized resolver.
     */
    protected abstract boolean isAuthorized(DataFetchingEnvironment environment) throws Exception;

    /**
     * Abstract method to be overridden by subclasses to implement a secured resolver.
     */
    protected abstract T authorizedGet(DataFetchingEnvironment environment) throws Exception;

    @Override
    final public T authenticatedGet(DataFetchingEnvironment environment) throws Exception {
        if (isAuthorized((environment))) {
            return authorizedGet(environment);
        }
        throw new AuthorizationError("Failed to authorize the current user.");
    }
}

