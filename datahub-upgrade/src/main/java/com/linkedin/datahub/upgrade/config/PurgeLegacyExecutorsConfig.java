package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.executors.PurgeLegacyExecutors;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCondition.BlockingSystemUpdateCondition.class)
public class PurgeLegacyExecutorsConfig {

  @Bean
  public BlockingSystemUpgrade purgeLegacyExecutors(
      final OperationContext opContext,
      final EntityService<?> entityService,
      final SearchService searchService,
      @Value("${systemUpdate.purgeLegacyExecutors.enabled}") final boolean enabled,
      @Value("${systemUpdate.purgeLegacyExecutors.reprocess.enabled}")
          final boolean reprocessEnabled,
      @Value("${systemUpdate.purgeLegacyExecutors.batchSize}") final Integer batchSize) {
    return new PurgeLegacyExecutors(
        opContext, entityService, searchService, enabled, reprocessEnabled, batchSize);
  }
}
