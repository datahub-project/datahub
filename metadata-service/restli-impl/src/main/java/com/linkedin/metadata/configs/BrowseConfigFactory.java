package com.linkedin.metadata.configs;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.browse.BaseBrowseConfig;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import javax.annotation.Nonnull;


public class BrowseConfigFactory {
  private BrowseConfigFactory() {
  }

  public static <T extends RecordTemplate> BaseBrowseConfig<T> getBrowseConfig(Class<T> clazz,
      IndexConvention indexConvention) {
    return new BaseBrowseConfig<T>() {
      @Override
      public Class<T> getSearchDocument() {
        return clazz;
      }

      @Nonnull
      @Override
      public String getIndexName() {
        return indexConvention.getIndexName(clazz);
      }
    };
  }
}
