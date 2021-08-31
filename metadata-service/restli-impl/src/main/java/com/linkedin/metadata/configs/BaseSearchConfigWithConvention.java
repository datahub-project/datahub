package com.linkedin.metadata.configs;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.search.BaseSearchConfig;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * {@link BaseSearchConfig} that uses {@link IndexConvention} to compute the name of the search index
 * @param <DOCUMENT>
 */
public abstract class BaseSearchConfigWithConvention<DOCUMENT extends RecordTemplate>
    extends BaseSearchConfig<DOCUMENT> {

  @Nullable
  private final IndexConvention _indexConvention;

  protected BaseSearchConfigWithConvention() {
    _indexConvention = null;
  }

  protected BaseSearchConfigWithConvention(@Nullable IndexConvention indexConvention) {
    _indexConvention = indexConvention;
  }

  @Nonnull
  @Override
  public String getIndexName() {
    if (_indexConvention == null) {
      return super.getIndexName();
    }
    return _indexConvention.getIndexName(getSearchDocument());
  }
}
