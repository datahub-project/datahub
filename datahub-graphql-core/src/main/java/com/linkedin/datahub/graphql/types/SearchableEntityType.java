package com.linkedin.datahub.graphql.types;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * Deprecated - this is no longer used in favor of the search and searchAcrossEntities GraphQL resolver.
 *
 *  Extension of {@link EntityType} containing methods required for 'search' functionality.
 *
 * @param <T>: The GraphQL object type corresponding to the entity, must extend the `Entity` interface.
 */
@Deprecated
public interface SearchableEntityType<T extends Entity, K> extends EntityType<T, K> {

    /**
     * Deprecated - this is no longer used in favor of the search and searchAcrossEntities GraphQL resolver.
     *
     * Retrieves {@link SearchResults} corresponding to a given query string, list of filters, start index, & count.
     *
     * @param query query text
     * @param filters list of filters that should be applied to search results, null if non were provided
     * @param start the initial offset of returned results
     * @param count the number of results to retrieve
     * @param context the {@link QueryContext} corresponding to the request.
     */
    @Deprecated
    SearchResults search(@Nonnull String query,
                         @Nullable List<FacetFilterInput> filters,
                         int start,
                         int count,
                         @Nonnull final QueryContext context) throws Exception;

    /**
     * Retrieves {@link AutoCompleteResults} corresponding to a given query string, field, list of filters, & limit.
     *
     * @param query query text
     * @param field the name of the field to autocomplete against, null if one was not provided
     * @param filters list of filters that should be applied to search results, null if non were provided
     * @param limit the maximum number of autocomplete suggestions to be returned
     * @param context the {@link QueryContext} corresponding to the request.
     */
    AutoCompleteResults autoComplete(@Nonnull String query,
                                     @Nullable String field,
                                     @Nullable List<FacetFilterInput> filters,
                                     int limit,
                                     @Nonnull final QueryContext context) throws Exception;

}
