package com.linkedin.datahub.graphql;

/**
 * Provided as input to GraphQL resolvers; used to carry information about GQL request context.
 */
public interface QueryContext {

    /**
     * Returns true if the current actor is authenticated, false otherwise.
     */
    boolean isAuthenticated();

    /**
     * Returns the current authenticated actor, null if there is none.
     */
    String getActor();
}
