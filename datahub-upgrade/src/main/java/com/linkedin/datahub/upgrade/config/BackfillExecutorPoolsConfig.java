package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.executorpools.ExecutorPools;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCondition.BlockingSystemUpdateCondition.class)
public class BackfillExecutorPoolsConfig {

  @Bean
  public BlockingSystemUpgrade backfillExecutorPools(
      final OperationContext opContext,
      final EntityService<?> entityService,
      final SearchService searchService,
      @Value("${systemUpdate.executorPools.enabled}") final boolean enabled,
      @Value("${systemUpdate.executorPools.reprocess.enabled}") final boolean reprocessEnabled,
      @Value("${systemUpdate.executorPools.batchSize}") final Integer batchSize,
      @Value("${systemUpdate.executorPools.customerId}") final String customerId) {
    return new ExecutorPools(
        opContext, entityService, searchService, enabled, reprocessEnabled, batchSize, customerId);
  }
}
