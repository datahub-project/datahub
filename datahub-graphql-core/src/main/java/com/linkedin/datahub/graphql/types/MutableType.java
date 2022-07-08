package com.linkedin.datahub.graphql.types;

import com.linkedin.datahub.graphql.QueryContext;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Graph type that can be updated.
 *
 * @param <I>: The input type corresponding to the write.
 */
public interface MutableType<I, T> {

    /**
     * Returns generated GraphQL class associated with the input type
     */
    Class<I> inputClass();

    default Class<I[]> arrayInputClass() throws UnsupportedOperationException {
        throw new UnsupportedOperationException(this.getClass().getName() + " does not implement arrayInputClass method");
    }


    /**
     * Update an entity by urn
     *
     * @param urn
     * @param input input type
     * @param context the {@link QueryContext} corresponding to the request.
     */
    T update(@Nonnull final String urn, @Nonnull final I input, @Nonnull final QueryContext context) throws Exception;
    
   /**
    * Update many entities
    *
    * @param input input type
    * @param context the {@link QueryContext} corresponding to the request.
    */
   default <T> List<T> batchUpdate(@Nonnull final String[] urns, @Nonnull final I[] input, @Nonnull final QueryContext context) throws Exception {
       throw new UnsupportedOperationException(this.getClass().getName() + " does not implement batchUpdate method");
   }
}
