package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.ingestion.BackfillIngestionSourceInfoIndices;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCondition.NonBlockingSystemUpdateCondition.class)
public class BackfillIngestionSourceInfoIndicesConfig {

  @Bean
  public NonBlockingSystemUpgrade backfillIngestionSourceInfoIndices(
      final OperationContext opContext,
      final EntityService<?> entityService,
      final AspectDao aspectDao,
      @Value("${systemUpdate.ingestionIndices.enabled}") final boolean enabled,
      @Value("${systemUpdate.ingestionIndices.batchSize}") final Integer batchSize,
      @Value("${systemUpdate.ingestionIndices.delayMs}") final Integer delayMs,
      @Value("${systemUpdate.ingestionIndices.limit}") final Integer limit) {
    return new BackfillIngestionSourceInfoIndices(
        opContext, entityService, aspectDao, enabled, batchSize, delayMs, limit);
  }
}
