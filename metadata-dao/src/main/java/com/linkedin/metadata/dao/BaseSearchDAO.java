package com.linkedin.metadata.dao;

import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.exception.ModelConversionException;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.validator.DocumentValidator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * A base class for all Search DAOs.
 *
 * A search DAO is a standardized interface to search metadata.
 * See http://go/gma for more details.
 *
 * @param <DOCUMENT> must be a search document type defined in com.linkedin.metadata.search
 */
public abstract class BaseSearchDAO<DOCUMENT extends RecordTemplate> {

  protected final Class<DOCUMENT> _documentClass;

  public BaseSearchDAO(Class<DOCUMENT> documentClass) {
    DocumentValidator.validateDocumentSchema(documentClass);
    _documentClass = documentClass;
  }

  /**
   * Gets a list of documents that match given search request. The results are aggregated and filters are applied to the
   * search hits and not the aggregation results.
   *
   * @param input the search input text
   * @param postFilters the request map with fields and values as filters to be applied to search hits
   * @param sortCriterion {@link SortCriterion} to be applied to search results
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @return a {@link SearchResult} that contains a list of matched documents and related search result metadata
   */
  @Nonnull
  public abstract SearchResult<DOCUMENT> search(@Nonnull String input, @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion, int from, int size);

  /**
   * Gets a list of documents after applying the input filters.
   *
   * @param filters the request map with fields and values to be applied as filters to the search query
   * @param sortCriterion {@link SortCriterion} to be applied to search results
   * @param from index to start the search from
   * @param size number of search hits to return
   * @return a {@link SearchResult} that contains a list of filtered documents and related search result metadata
   */
  @Nonnull
  public abstract SearchResult<DOCUMENT> filter(@Nullable Filter filters, @Nullable SortCriterion sortCriterion, int from, int size);

  /**
   * Returns a list of suggestions given type ahead query
   *
   * The advanced auto complete can take filters and provides suggestions based on filtered context
   *
   * @param query the type ahead query text
   * @param field the field name for the auto complete
   * @param requestParams specify the field to auto complete and the input text
   * @param limit the number of suggestions returned
   * @return A list of suggestions as string
   */
  @Nonnull
  public abstract AutoCompleteResult autoComplete(@Nonnull String query, @Nullable String field,
      @Nullable Filter requestParams, int limit);


  @Nonnull
  protected DOCUMENT newDocument(@Nonnull DataMap dataMap) {
    try {
      return _documentClass.getConstructor(DataMap.class).newInstance(dataMap);
    } catch (Exception ex) {
      throw new ModelConversionException("Metadata Conversion error", ex);
    }
  }

}
