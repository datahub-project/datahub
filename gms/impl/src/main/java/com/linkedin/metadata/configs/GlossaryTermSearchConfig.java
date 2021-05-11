package com.linkedin.metadata.configs;

import com.linkedin.metadata.dao.search.BaseSearchConfig;
import com.linkedin.metadata.dao.utils.SearchUtils;
import com.linkedin.metadata.search.GlossaryTermInfoDocument;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nonnull;


public class GlossaryTermSearchConfig extends BaseSearchConfig<GlossaryTermInfoDocument> {
  @Override
  @Nonnull
  public Set<String> getFacetFields() {
    return Collections.emptySet();
  }

  @Override
  @Nonnull
  public Class<GlossaryTermInfoDocument> getSearchDocument() {
    return GlossaryTermInfoDocument.class;
  }

  @Override
  @Nonnull
  public String getDefaultAutocompleteField() {
    return "definition";
  }

  @Override
  @Nonnull
  public String getSearchQueryTemplate() {
    return SearchUtils.readResourceFile(getClass(), "glossaryTermESSearchQueryTemplate.json");
  }

  @Override
  @Nonnull
  public String getAutocompleteQueryTemplate() {
    return SearchUtils.readResourceFile(getClass(), "glossaryTermESAutocompleteQueryTemplate.json");
  }
}