package com.linkedin.metadata.configs;

import com.linkedin.metadata.dao.utils.SearchUtils;
import com.linkedin.metadata.search.MLModelDocument;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nonnull;


public class MLModelSearchConfig extends BaseSearchConfigWithConvention<MLModelDocument> {
  public MLModelSearchConfig() {
  }

  public MLModelSearchConfig(IndexConvention indexConvention) {
    super(indexConvention);
  }

  @Override
  @Nonnull
  public Set<String> getFacetFields() {
    return Collections.emptySet();
  }

  @Override
  @Nonnull
  public Class<MLModelDocument> getSearchDocument() {
    return MLModelDocument.class;
  }

  @Override
  @Nonnull
  public String getDefaultAutocompleteField() {
    return "name";
  }

  @Override
  @Nonnull
  public String getSearchQueryTemplate() {
    return SearchUtils.readResourceFile(getClass(), "mlModelESSearchQueryTemplate.json");
  }

  @Override
  @Nonnull
  public String getAutocompleteQueryTemplate() {
    return SearchUtils.readResourceFile(getClass(), "mlModelESAutocompleteQueryTemplate.json");
  }
}
