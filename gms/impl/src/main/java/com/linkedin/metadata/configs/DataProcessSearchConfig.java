package com.linkedin.metadata.configs;

import com.linkedin.metadata.dao.utils.SearchUtils;
import com.linkedin.metadata.search.DataProcessDocument;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nonnull;


public class DataProcessSearchConfig extends BaseSearchConfigWithConvention<DataProcessDocument> {
  public DataProcessSearchConfig() {
  }

  public DataProcessSearchConfig(IndexConvention indexConvention) {
    super(indexConvention);
  }

  @Override
  @Nonnull
  public Set<String> getFacetFields() {
    return Collections.emptySet();
  }

  @Override
  @Nonnull
  public Class<DataProcessDocument> getSearchDocument() {
    return DataProcessDocument.class;
  }

  @Override
  @Nonnull
  public String getDefaultAutocompleteField() {
    return "name";
  }

  @Override
  @Nonnull
  public String getSearchQueryTemplate() {
    return SearchUtils.readResourceFile(getClass(), "dataProcessESSearchQueryTemplate.json");
  }

  @Override
  @Nonnull
  public String getAutocompleteQueryTemplate() {
    return SearchUtils.readResourceFile(getClass(), "dataProcessESAutocompleteQueryTemplate.json");
  }
}
