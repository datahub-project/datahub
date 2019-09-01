package com.linkedin.metadata.restli;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.SimpleResourceTaskTemplate;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.restli.RestliConstants.*;


/**
 * A base class for autocomplete rest.li resource
 *
 * @param <DOCUMENT> must be a valid document model defined in com.linkedin.metadata.search
 * @deprecated Use {@link BaseSearchableEntityResource} instead
 */
public abstract class BaseAutocompleteResource<DOCUMENT extends RecordTemplate>
    extends SimpleResourceTaskTemplate<AutoCompleteResult> {

  /**
   * Returns a document-specific {@link BaseSearchDAO}.
   */
  @Nonnull
  protected abstract BaseSearchDAO<DOCUMENT> getSearchDAO();

  /**
   * Gets auto complete result given type ahead request
   *
   * This abstract method is defined to enforce child class to override it.
   * If we don't define it this way and child class doesn't override,
   * resource will not be created because Rest.li doesn't support resource annotation in abstract class.
   * Child class' overriding method should call getAutoCompleteResult method.
   *
   * @param input the search input text
   * @param field the field name for the auto complete
   * @param filter filter context
   * @param limit the number of suggestions returned
   * @return auto complete result
   */
  @RestMethod.Get
  @Nonnull
  protected abstract Task<AutoCompleteResult> autoComplete(@QueryParam(PARAM_INPUT) @Nonnull String input,
      @QueryParam(PARAM_FIELD) @Nonnull String field, @QueryParam(PARAM_FILTER) @Nonnull Filter filter,
      @QueryParam(PARAM_LIMIT) int limit);

  @Nonnull
  protected Task<AutoCompleteResult> getAutoCompleteResult(@Nonnull String input,
      @Nonnull String field, @Nonnull Filter filter, int limit) {
    return RestliUtils.toTask(() -> getSearchDAO().autoComplete(input, field, filter, limit));
  }
}
