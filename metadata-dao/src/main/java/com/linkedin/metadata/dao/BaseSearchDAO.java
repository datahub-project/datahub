package com.linkedin.metadata.dao;

import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.exception.ModelConversionException;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.validator.DocumentValidator;
import javax.annotation.Nonnull;


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
    DocumentValidator.validateSchema(documentClass);
    _documentClass = documentClass;
  }

  /**
   * Gets a list of documents that match given search request
   *
   * @param input the search input text
   * @param requestParams the request map with fields and values as filters
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @return a {@link SearchResult} that contains a list of matched documents and related search result metadata
   */
  @Nonnull
  public abstract SearchResult<DOCUMENT> search(@Nonnull String input, @Nonnull Filter requestParams, int from,
      int size);

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
  public abstract AutoCompleteResult autoComplete(@Nonnull String query, @Nonnull String field,
      @Nonnull Filter requestParams, int limit);


  @Nonnull
  protected DOCUMENT newDocument(@Nonnull DataMap dataMap) {
    try {
      return _documentClass.getConstructor(DataMap.class).newInstance(dataMap);
    } catch (Exception ex) {
      throw new ModelConversionException("Metadata Conversion error", ex);
    }
  }

}
