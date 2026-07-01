package com.linkedin.datahub.upgrade.config.restoreindices;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.restoreindices.dbtsiblingsindices.ReindexDbtSiblings;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCondition.NonBlockingSystemUpdateCondition.class)
public class ReindexDbtSiblingsConfig {

  @Bean
  public NonBlockingSystemUpgrade reindexDbtSiblings(
      final OperationContext opContext,
      final EntityService<?> entityService,
      final AspectDao aspectDao,
      @Value("${systemUpdate.restoreDbtSiblingsIndices.enabled}") final boolean enabled,
      @Value("${systemUpdate.restoreDbtSiblingsIndices.batchSize}") final Integer batchSize,
      @Value("${systemUpdate.restoreDbtSiblingsIndices.delayMs}") final Integer delayMs,
      @Value("${systemUpdate.restoreDbtSiblingsIndices.limit}") final Integer limit) {
    return new ReindexDbtSiblings(
        opContext, entityService, aspectDao, enabled, batchSize, delayMs, limit);
  }
}
