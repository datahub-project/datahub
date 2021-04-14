package com.linkedin.metadata.configs;

import com.linkedin.metadata.dao.utils.SearchUtils;
import com.linkedin.metadata.search.CorpUserInfoDocument;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nonnull;


public class CorpUserSearchConfig extends BaseSearchConfigWithConvention<CorpUserInfoDocument> {
  public CorpUserSearchConfig() {
  }

  public CorpUserSearchConfig(IndexConvention indexConvention) {
    super(indexConvention);
  }

  @Override
  @Nonnull
  public Set<String> getFacetFields() {
    return Collections.emptySet();
  }

  @Override
  @Nonnull
  public Class<CorpUserInfoDocument> getSearchDocument() {
    return CorpUserInfoDocument.class;
  }

  @Override
  @Nonnull
  public String getDefaultAutocompleteField() {
    return "fullName";
  }

  @Override
  @Nonnull
  public String getSearchQueryTemplate() {
    return SearchUtils.readResourceFile(getClass(), "corpUserESSearchQueryTemplate.json");
  }

  @Override
  @Nonnull
  public String getAutocompleteQueryTemplate() {
    return SearchUtils.readResourceFile(getClass(), "corpUserESAutocompleteQueryTemplate.json");
  }
}