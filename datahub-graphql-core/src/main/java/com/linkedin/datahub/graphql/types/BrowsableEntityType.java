package com.linkedin.datahub.graphql.types;

import com.linkedin.datahub.graphql.QueryContext;
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
 * @param <K> the key type for the DataLoader
 */
public interface BrowsableEntityType<T extends Entity, K> extends EntityType<T, K> {

    /**
     * Retrieves {@link BrowseResults} corresponding to a given path, list of filters, start, & count.
     *
     * @param path the path to find browse results under
     * @param filters list of filters that should be applied to search results, null if non were provided
     * @param start the initial offset of returned results
     * @param count the number of results to retrieve
     * @param context the {@link QueryContext} corresponding to the request.
     */
    @Nonnull
    BrowseResults browse(@Nonnull List<String> path,
                         @Nullable List<FacetFilterInput> filters,
                         int start,
                         int count,
                         @Nonnull final QueryContext context) throws Exception;

    /**
     * Retrieves a list of {@link BrowsePath} corresponding to a given path, list of filters, start, & count.
     *
     * @param urn the entity urn to fetch browse paths for
     * @param context the {@link QueryContext} corresponding to the request.
     */
    @Nonnull
    List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull final QueryContext context) throws Exception;

}
