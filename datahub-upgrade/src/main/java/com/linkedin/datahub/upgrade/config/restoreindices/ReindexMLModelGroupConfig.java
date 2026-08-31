package com.linkedin.datahub.upgrade.config.restoreindices;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.restoreindices.mlmodelgroup.ReindexMLModelGroup;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCondition.NonBlockingSystemUpdateCondition.class)
public class ReindexMLModelGroupConfig {

  @Bean
  public NonBlockingSystemUpgrade reindexMLModelGroup(
      final OperationContext opContext,
      final EntityService<?> entityService,
      final AspectDao aspectDao,
      @Value("${systemUpdate.mlModelGroup.enabled}") final boolean enabled,
      @Value("${systemUpdate.mlModelGroup.batchSize}") final Integer batchSize,
      @Value("${systemUpdate.mlModelGroup.delayMs}") final Integer delayMs,
      @Value("${systemUpdate.mlModelGroup.limit}") final Integer limit) {
    return new ReindexMLModelGroup(
        opContext, entityService, aspectDao, enabled, batchSize, delayMs, limit);
  }
}
