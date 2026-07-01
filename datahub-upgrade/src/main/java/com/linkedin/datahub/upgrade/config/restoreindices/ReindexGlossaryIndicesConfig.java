package com.linkedin.datahub.upgrade.config.restoreindices;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.restoreindices.glossary.ReindexGlossaryIndices;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCondition.NonBlockingSystemUpdateCondition.class)
public class ReindexGlossaryIndicesConfig {

  @Bean
  public NonBlockingSystemUpgrade reindexGlossaryIndices(
      final OperationContext opContext,
      final EntityService<?> entityService,
      final AspectDao aspectDao,
      @Value("${systemUpdate.restoreGlossaryIndices.enabled}") final boolean enabled,
      @Value("${systemUpdate.restoreGlossaryIndices.batchSize}") final Integer batchSize,
      @Value("${systemUpdate.restoreGlossaryIndices.delayMs}") final Integer delayMs,
      @Value("${systemUpdate.restoreGlossaryIndices.limit}") final Integer limit) {
    return new ReindexGlossaryIndices(
        opContext, entityService, aspectDao, enabled, batchSize, delayMs, limit);
  }
}
