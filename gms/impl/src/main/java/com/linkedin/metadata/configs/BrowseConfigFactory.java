package com.linkedin.metadata.configs;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.browse.BaseBrowseConfig;


public class BrowseConfigFactory {
  private BrowseConfigFactory() {
  }
  
  public static <T extends RecordTemplate> BaseBrowseConfig<T> getBrowseConfig(Class<T> clazz) {
    return new BaseBrowseConfig<T>() {
      @Override
      public Class<T> getSearchDocument() {
        return clazz;
      }
    };
  }
}
