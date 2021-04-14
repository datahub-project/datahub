package com.linkedin.metadata.configs;

import com.linkedin.metadata.dao.utils.SearchUtils;
import com.linkedin.metadata.search.TagDocument;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;


public class TagSearchConfig extends BaseSearchConfigWithConvention<TagDocument> {
  public TagSearchConfig() {
  }

  public TagSearchConfig(IndexConvention indexConvention) {
    super(indexConvention);
  }

  @Nonnull
  @Override
  public Set<String> getFacetFields() {
    return Collections.unmodifiableSet(new HashSet<>(Arrays.asList()));
  }

  @Nonnull
  @Override
  public Class<TagDocument> getSearchDocument() {
    return TagDocument.class;
  }

  @Nonnull
  @Override
  public String getDefaultAutocompleteField() {
    return "name";
  }

  @Nonnull
  @Override
  public String getSearchQueryTemplate() {
    return SearchUtils.readResourceFile(getClass(), "tagESSearchQueryTemplate.json");
  }

  @Nonnull
  @Override
  public String getAutocompleteQueryTemplate() {
    return SearchUtils.readResourceFile(getClass(), "tagESAutocompleteQueryTemplate.json");
  }
}
