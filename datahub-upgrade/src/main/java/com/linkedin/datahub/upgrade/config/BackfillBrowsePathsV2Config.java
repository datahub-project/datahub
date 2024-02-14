package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.system.entity.steps.BackfillBrowsePathsV2;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BackfillBrowsePathsV2Config {

  @Bean
  public BackfillBrowsePathsV2 backfillBrowsePathsV2(
      final OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      @Value("${systemUpdate.browsePathsV2.enabled}") final boolean enabled,
      @Value("${systemUpdate.browsePathsV2.reprocess.enabled}") final boolean reprocessEnabled,
      @Value("${systemUpdate.browsePathsV2.batchSize}") final Integer batchSize) {
    return new BackfillBrowsePathsV2(
        opContext, entityService, searchService, enabled, reprocessEnabled, batchSize);
  }
}
