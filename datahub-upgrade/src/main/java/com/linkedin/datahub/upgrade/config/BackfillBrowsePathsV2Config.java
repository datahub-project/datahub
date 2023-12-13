package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.system.entity.steps.BackfillBrowsePathsV2;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BackfillBrowsePathsV2Config {

  @Bean
  public BackfillBrowsePathsV2 backfillBrowsePathsV2(
      EntityService entityService, SearchService searchService) {
    return new BackfillBrowsePathsV2(entityService, searchService);
  }
}
