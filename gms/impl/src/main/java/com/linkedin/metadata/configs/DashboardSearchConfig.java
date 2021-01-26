package com.linkedin.metadata.configs;

import com.linkedin.metadata.dao.search.BaseSearchConfig;
import com.linkedin.metadata.dao.utils.SearchUtils;
import com.linkedin.metadata.search.DashboardDocument;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;


public class DashboardSearchConfig extends BaseSearchConfig<DashboardDocument> {
  @Nonnull
  @Override
  public Set<String> getFacetFields() {
    return Collections.unmodifiableSet(new HashSet<>(Arrays.asList("access", "tool")));
  }

  @Nonnull
  @Override
  public Class<DashboardDocument> getSearchDocument() {
    return DashboardDocument.class;
  }

  @Nonnull
  @Override
  public String getDefaultAutocompleteField() {
    return "title";
  }

  @Nonnull
  @Override
  public String getSearchQueryTemplate() {
    return SearchUtils.readResourceFile(getClass(), "dashboardESSearchQueryTemplate.json");
  }

  @Nonnull
  @Override
  public String getAutocompleteQueryTemplate() {
    return SearchUtils.readResourceFile(getClass(), "dashboardESAutocompleteQueryTemplate.json");
  }
}
