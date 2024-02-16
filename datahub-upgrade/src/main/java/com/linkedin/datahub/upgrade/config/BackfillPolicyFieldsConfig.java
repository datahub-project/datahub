package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.system.entity.steps.BackfillBrowsePathsV2;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class BackfillPolicyFieldsConfig {

  @Bean
  public BackfillBrowsePathsV2 backfillBrowsePathsV2(EntityService<?> entityService, SearchService searchService,
      @Value("${systemUpdate.policyFields.enabled}") final boolean enabled,
      @Value("${systemUpdate.policyFields.reprocess.enabled}") final boolean reprocessEnabled,
      @Value("${systemUpdate.policyFields.batchSize}") final Integer batchSize) {
    return new BackfillBrowsePathsV2(entityService, searchService, enabled, reprocessEnabled, batchSize);
  }
}