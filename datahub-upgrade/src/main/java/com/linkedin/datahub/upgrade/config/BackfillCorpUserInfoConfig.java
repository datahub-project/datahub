package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.corpuserinfo.BackfillCorpUserInfo;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCondition.NonBlockingSystemUpdateCondition.class)
public class BackfillCorpUserInfoConfig {

  @Bean
  public NonBlockingSystemUpgrade backfillcorpUserInfo(
      final OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      @Value("${systemUpdate.corpUserInfo.enabled}") final boolean enabled,
      @Value("${systemUpdate.corpUserInfo.reprocess.enabled}") final boolean reprocessEnabled,
      @Value("${systemUpdate.corpUserInfo.batchSize}") final Integer batchSize) {
    return new BackfillCorpUserInfo(
        opContext, entityService, searchService, enabled, reprocessEnabled, batchSize);
  }
}
