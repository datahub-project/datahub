package com.linkedin.datahub.graphql.types;


import com.linkedin.datahub.graphql.QueryContext;

import javax.annotation.Nonnull;

/**
 * GQL graph type that can be updated.
 *
 * @param <I>: The input type corresponding to the write.
 * @param <T>: The output type resulting from the update.

 */
public interface WritableType<I> {

    /**
     * Returns generated GraphQL class associated with the input type
     */
    Class<I> inputClass();


    /**
     * Update an entity by urn
     *
     * @param input input type
     * @param context the {@link QueryContext} corresponding to the request.
     */
    <T> T update(@Nonnull final I input, @Nonnull final QueryContext context) throws Exception;
}
