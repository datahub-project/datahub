package com.linkedin.metadata.configs;

import com.linkedin.metadata.dao.search.BaseSearchConfig;
import com.linkedin.metadata.dao.utils.SearchUtils;
import com.linkedin.metadata.search.GlossaryNodeInfoDocument;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;


public class GlossaryNodeSearchConfig extends BaseSearchConfig<GlossaryNodeInfoDocument> {
  @Override
  @Nonnull
  public Set<String> getFacetFields() {
    return Collections.emptySet();
  }

  @Override
  @Nonnull
  public Class<GlossaryNodeInfoDocument> getSearchDocument() {
    return GlossaryNodeInfoDocument.class;
  }

  @Override
  @Nonnull
  public String getDefaultAutocompleteField() {
    return "definition";
  }

  @Override
  @Nonnull
  public String getSearchQueryTemplate() {
    return SearchUtils.readResourceFile(getClass(), "glossaryNodeESSearchQueryTemplate.json");
  }

  @Override
  @Nonnull
  public String getAutocompleteQueryTemplate() {
    return SearchUtils.readResourceFile(getClass(), "glossaryNodeESAutocompleteQueryTemplate.json");
  }
}