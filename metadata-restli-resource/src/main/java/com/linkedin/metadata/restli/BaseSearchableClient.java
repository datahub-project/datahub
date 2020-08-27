package com.linkedin.metadata.restli;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.common.CollectionResponse;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Base client that all entities supporting search should implement in their respective restli MPs
 * @param <VALUE> the client's value type
 */
public abstract class BaseSearchableClient<VALUE extends RecordTemplate> extends BaseClient {

  public BaseSearchableClient(@Nonnull Client restliClient) {
    super(restliClient);
  }

  /**
   * Search method that the client inheriting this class must implement. Returns {@link CollectionResponse} containing list of aspects
   *
   * @param input Input query
   * @param aspectNames List of aspects to be returned in the VALUE model
   * @param requestFilters Request map with fields and values to be applied as filters to the search query
   * @param sortCriterion {@link SortCriterion} to be applied to search results
   * @param start Start of the page
   * @param count Number of search results to return
   * @return {@link CollectionResponse} of VALUE for the records that satisfy the given search query
   * @throws RemoteInvocationException when the rest.li request fails
   */
  @Nonnull
  public abstract CollectionResponse<VALUE> search(@Nonnull String input, @Nullable StringArray aspectNames, @Nullable Map<String, String> requestFilters,
      @Nullable SortCriterion sortCriterion, int start, int count) throws RemoteInvocationException;

  /**
   * Similar to {@link #search(String, StringArray, Map, SortCriterion, int, int)} with null for aspect names, meaning all aspects will be returned
   */
  @Nonnull
  public CollectionResponse<VALUE> search(@Nonnull String input, @Nullable Map<String, String> requestFilters,
      @Nullable SortCriterion sortCriterion, int start, int count) throws RemoteInvocationException {
    return search(input, null, requestFilters, sortCriterion, start, count);
  }

  /**
   * Autocomplete method that the client will override only if they need this capability. It returns {@link AutoCompleteResult} containing list of suggestions.
   *
   * @param query Input query
   * @param field Field against which the query needs autocompletion
   * @param requestFilters Request map with fields and values to be applied as filters to the autocomplete query
   * @param limit Number of suggestions returned
   * @return {@link AutoCompleteResult} containing list of suggestions as strings
   * @throws RemoteInvocationException when the rest.li request fails
   */
  @Nonnull
  public AutoCompleteResult autocomplete(@Nonnull String query, @Nullable String field,
      @Nullable Map<String, String> requestFilters, int limit) throws RemoteInvocationException {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

}