package com.linkedin.metadata.configs;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.dao.utils.SearchUtils;
import com.linkedin.metadata.search.DatasetDocument;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class DatasetSearchConfig extends BaseSearchConfigWithConvention<DatasetDocument> {
  public DatasetSearchConfig() {
  }

  public DatasetSearchConfig(IndexConvention indexConvention) {
    super(indexConvention);
  }

  @Override
  @Nonnull
  public Set<String> getFacetFields() {
    return Collections.unmodifiableSet(new HashSet<>(Arrays.asList("origin", "platform")));
  }

  @Nonnull
  public Class getSearchDocument() {
    return DatasetDocument.class;
  }

  @Override
  @Nonnull
  public String getDefaultAutocompleteField() {
    return "name";
  }

  @Override
  @Nonnull
  public String getSearchQueryTemplate() {
    return SearchUtils.readResourceFile(getClass(), "datasetESSearchQueryTemplate.json");
  }

  @Override
  @Nonnull
  public String getAutocompleteQueryTemplate() {
    return SearchUtils.readResourceFile(getClass(), "datasetESAutocompleteQueryTemplate.json");
  }

  @Override
  @Nullable
  public List<String> getFieldsToHighlightMatch() {
    return ImmutableList.of("name", "fieldPaths");
  }
}
