package com.linkedin.datahub.graphql.types;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * GQL graph type that can be loaded from a downstream service by primary key.
 *
 * @param <T>: The GraphQL object type corresponding to the entity.
 */
public interface LoadableType<T> {

    /**
     * Returns generated GraphQL class associated with the type
     */
    Class<T> objectClass();

    /**
     * Returns the name of the type, to be used in creating a corresponding GraphQL {@link org.dataloader.DataLoader}
     */
    default String name() {
        return objectClass().getSimpleName();
    }

    /**
     * Retrieves an entity by urn string. Null is provided in place of an entity object if an entity cannot be found.
     *
     * @param urn to retrieve
     */
    default T load(@Nonnull final String urn) throws Exception {
        return batchLoad(ImmutableList.of(urn)).get(0);
    };

    /**
     * Retrieves an list of entities given a list of urn strings. The list returned is expected to
     * be of same length of the list of urns, where nulls are provided in place of an entity object if an entity cannot be found.
     *
     * @param urns to retrieve
     */
    List<T> batchLoad(@Nonnull final List<String> urns) throws Exception;
}
