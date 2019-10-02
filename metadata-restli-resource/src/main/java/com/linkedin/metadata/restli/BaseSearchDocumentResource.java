package com.linkedin.metadata.restli;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.SearchResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.annotations.Finder;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.PagingContextParam;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.metadata.restli.RestliConstants.*;


/**
 * A base class for search document rest.li resource
 *
 * @param <DOCUMENT> must be a valid document model defined in com.linkedin.metadata.search
 * @deprecated Use {@link BaseSearchableEntityResource} instead
 */
public abstract class BaseSearchDocumentResource<DOCUMENT extends RecordTemplate>
    extends CollectionResourceTaskTemplate<String, DOCUMENT> {

  /**
   * Returns a document-specific {@link BaseSearchDAO}.
   */
  @Nonnull
  protected abstract BaseSearchDAO<DOCUMENT> getSearchDAO();

  /**
   * Gets a list of search documents given search request
   *
   * This abstract method is defined to enforce child class to override it.
   * If we don't define it this way and child class doesn't override,
   * resource will not be created because Rest.li doesn't support resource annotation in abstract class.
   * Child class' overriding method should call getAutoCompleteResult method.
   *
   * @param input search input text
   * @param filter search request
   * @param pagingContext the pagination context
   * @return a list of matching search documents and its related search metadata
   */
  @Finder("search")
  @Nonnull
  protected abstract Task<CollectionResult<DOCUMENT, SearchResultMetadata>> search(
      @QueryParam(PARAM_INPUT) @Nonnull String input, @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion,
      @PagingContextParam @Nonnull PagingContext pagingContext);

  @Nonnull
  protected Task<CollectionResult<DOCUMENT, SearchResultMetadata>> getSearchDocuments(@Nonnull String input,
      @Nullable Filter filter, @Nullable SortCriterion sortCriterion, @Nonnull PagingContext pagingContext) {
    return RestliUtils.toTask(() -> {
      final SearchResult<DOCUMENT> searchResult =
          getSearchDAO().search(input, filter, sortCriterion, pagingContext.getStart(), pagingContext.getCount());

      return new CollectionResult<>(searchResult.getDocumentList(), searchResult.getTotalCount(),
          searchResult.getSearchResultMetadata());
    });
  }
}
