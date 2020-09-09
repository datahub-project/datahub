package com.linkedin.metadata.dao.search;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;


@Slf4j
public abstract class BaseESAutoCompleteQuery {

  public BaseESAutoCompleteQuery() {
  }

  /**
   * Constructs the search query for auto complete request
   *
   * @param field the field name for the auto complete
   * @param input the type ahead query text
   * @param requestParams the request map as filters
   * @return a valid search request
   * TODO: merge this with regular search query construction to take filters as context for suggestions
   */
  @Nonnull
  abstract SearchRequest constructAutoCompleteQuery(@Nonnull String input, @Nonnull String field,
      @Nullable Filter requestParams);

  /**
   * Gets a list of suggestions out of raw search hits
   *
   * @param searchResponse the raw search response from search engine
   * @param field the field name for the auto complete
   * @param input the string that needs to be completed
   * @param limit number of suggestions to return
   * @return A list of suggestion strings
   */
  @Nonnull
  abstract StringArray getSuggestionList(@Nonnull SearchResponse searchResponse, @Nonnull String field,
      @Nonnull String input, int limit);

  @VisibleForTesting
  @Nonnull
  AutoCompleteResult extractAutoCompleteResult(@Nonnull SearchResponse searchResponse, @Nonnull String input,
      @Nonnull String field, int limit) {

    return new AutoCompleteResult().setQuery(input)
        .setSuggestions(getSuggestionList(searchResponse, field, input, limit));
  }

  @Nonnull
  String getAutocompleteQueryTemplate() {
    return "";
  }
}
