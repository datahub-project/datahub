package com.linkedin.metadata.dao.search;

import com.linkedin.data.template.RecordTemplate;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public abstract class BaseSearchConfig<DOCUMENT extends RecordTemplate> {

  @Nonnull
  public abstract Set<String> getFacetFields();

  @Nullable
  public Set<String> getLowCardinalityFields() {
    return null;
  }

  @Nonnull
  public String getIndexName() {
    return getSearchDocument().getSimpleName().toLowerCase();
  }

  @Nonnull
  public abstract Class<DOCUMENT> getSearchDocument();

  @Nonnull
  public abstract String getDefaultAutocompleteField();

  @Nonnull
  public abstract String getSearchQueryTemplate();

  @Nonnull
  public abstract String getAutocompleteQueryTemplate();
}
