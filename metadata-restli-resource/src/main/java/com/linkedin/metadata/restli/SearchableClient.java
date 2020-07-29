package com.linkedin.metadata.restli;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.common.CollectionResponse;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Interface that all entities that support search and autocomplete should implement in their respective restli MPs
 * @param <VALUE> the client's value type
 *
 * @deprecated Use {@link BaseSearchableClient} instead
 */
public interface SearchableClient<VALUE extends RecordTemplate> {

  @Nonnull
  CollectionResponse<VALUE> search(@Nonnull String input, @Nullable Map<String, String> requestFilters,
      @Nullable SortCriterion sortCriterion, int start, int count) throws RemoteInvocationException;

  @Nonnull
  AutoCompleteResult autocomplete(@Nonnull String query, @Nullable String field,
      @Nullable Map<String, String> requestFilters, int limit) throws RemoteInvocationException;

}