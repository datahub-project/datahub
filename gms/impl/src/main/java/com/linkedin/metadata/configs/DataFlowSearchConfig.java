package com.linkedin.metadata.configs;

import com.linkedin.metadata.dao.utils.SearchUtils;
import com.linkedin.metadata.search.DataFlowDocument;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nonnull;


public class DataFlowSearchConfig extends BaseSearchConfigWithConvention<DataFlowDocument> {
  public DataFlowSearchConfig() {
  }

  public DataFlowSearchConfig(IndexConvention indexConvention) {
    super(indexConvention);
  }

  @Override
  @Nonnull
  public Set<String> getFacetFields() {
    return Collections.emptySet();
  }

  @Override
  @Nonnull
  public Class<DataFlowDocument> getSearchDocument() {
    return DataFlowDocument.class;
  }

  @Override
  @Nonnull
  public String getDefaultAutocompleteField() {
    return "flowId";
  }

  @Override
  @Nonnull
  public String getSearchQueryTemplate() {
    return SearchUtils.readResourceFile(getClass(), "dataFlowESSearchQueryTemplate.json");
  }

  @Override
  @Nonnull
  public String getAutocompleteQueryTemplate() {
    return SearchUtils.readResourceFile(getClass(), "dataFlowESAutocompleteQueryTemplate.json");
  }
}
