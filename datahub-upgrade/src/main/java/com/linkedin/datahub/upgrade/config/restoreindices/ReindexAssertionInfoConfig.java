package com.linkedin.datahub.upgrade.config.restoreindices;

import com.linkedin.datahub.upgrade.config.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.restoreindices.assertions.ReindexAssertionInfo;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCondition.NonBlockingSystemUpdateCondition.class)
public class ReindexAssertionInfoConfig {

  @Bean
  public NonBlockingSystemUpgrade reindexAssertionInfo(
      final OperationContext opContext,
      final EntityService<?> entityService,
      final AspectDao aspectDao,
      @Value("${systemUpdate.assertionInfo.enabled}") final boolean enabled,
      @Value("${systemUpdate.assertionInfo.batchSize}") final Integer batchSize,
      @Value("${systemUpdate.assertionInfo.delayMs}") final Integer delayMs,
      @Value("${systemUpdate.assertionInfo.limit}") final Integer limit) {
    return new ReindexAssertionInfo(
        opContext, entityService, aspectDao, enabled, batchSize, delayMs, limit);
  }
}
