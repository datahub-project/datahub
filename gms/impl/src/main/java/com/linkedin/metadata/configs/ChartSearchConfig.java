package com.linkedin.metadata.configs;

import com.linkedin.metadata.dao.utils.SearchUtils;
import com.linkedin.metadata.search.ChartDocument;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;


public class ChartSearchConfig extends BaseSearchConfigWithConvention<ChartDocument> {
  public ChartSearchConfig() {
  }

  public ChartSearchConfig(IndexConvention indexConvention) {
    super(indexConvention);
  }

  @Nonnull
  @Override
  public Set<String> getFacetFields() {
    return Collections.unmodifiableSet(new HashSet<>(Arrays.asList("access", "queryType", "tool", "type")));
  }

  @Nonnull
  @Override
  public Class<ChartDocument> getSearchDocument() {
    return ChartDocument.class;
  }

  @Nonnull
  @Override
  public String getDefaultAutocompleteField() {
    return "title";
  }

  @Nonnull
  @Override
  public String getSearchQueryTemplate() {
    return SearchUtils.readResourceFile(getClass(), "chartESSearchQueryTemplate.json");
  }

  @Nonnull
  @Override
  public String getAutocompleteQueryTemplate() {
    return SearchUtils.readResourceFile(getClass(), "chartESAutocompleteQueryTemplate.json");
  }
}
