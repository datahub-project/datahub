package com.linkedin.datahub.graphql.types;

import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * Extension of {@link EntityType} containing methods required for 'browse' functionality.
 *
 * @param <T>: The GraphQL object type corresponding to the entity, must extend the `Entity` interface.
 */
public interface BrowsableEntityType<T extends Entity> extends EntityType<T> {

    /**
     * Retrieves {@link BrowseResults} corresponding to a given path, list of filters, start, & count.
     *
     * @param path the path to find browse results under
     * @param filters list of filters that should be applied to search results, null if non were provided
     * @param start the initial offset of returned results
     * @param count the number of results to retrieve
     */
    default BrowseResults browse(@Nonnull List<String> path,
                                 @Nullable List<FacetFilterInput> filters,
                                 int start,
                                 int count) throws Exception {
        throw new UnsupportedOperationException();
    }

    /**
     * Retrieves a list of {@link BrowsePath} corresponding to a given path, list of filters, start, & count.
     *
     * @param urn the entity urn to fetch browse paths for
     */
    default List<BrowsePath> browsePaths(@Nonnull String urn) throws Exception {
        throw new UnsupportedOperationException();
    }

}
