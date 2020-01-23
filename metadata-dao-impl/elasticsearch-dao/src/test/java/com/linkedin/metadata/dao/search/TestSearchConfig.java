package com.linkedin.metadata.dao.search;

import com.linkedin.testing.EntityDocument;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;

public class TestSearchConfig extends BaseSearchConfig<EntityDocument> {
  @Override
  @Nonnull
  public Set<String> getFacetFields() {
    return Collections.unmodifiableSet(new HashSet<>());
  }

  @Override
  @Nonnull
  public Class<EntityDocument> getSearchDocument() {
    return EntityDocument.class;
  }

  @Override
  @Nonnull
  public String getDefaultAutocompleteField() {
    return "urn";
  }

  @Override
  @Nonnull
  public String getSearchQueryTemplate() {
    return "";
  }

  @Override
  @Nonnull
  public String getAutocompleteQueryTemplate() {
    return "";
  }
}
