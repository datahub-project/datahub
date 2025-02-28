package com.linkedin.datahub.upgrade.config.restoreindices.graph;

import com.linkedin.datahub.upgrade.config.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.restoreindices.graph.vianodes.ReindexDataJobViaNodesCLL;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCondition.NonBlockingSystemUpdateCondition.class)
public class ReindexDataJobViaNodesCLLConfig {

  @Bean
  public NonBlockingSystemUpgrade reindexDataJobViaNodesCLL(
      final OperationContext opContext,
      final EntityService<?> entityService,
      final AspectDao aspectDao,
      @Value("${systemUpdate.dataJobNodeCLL.enabled}") final boolean enabled,
      @Value("${systemUpdate.dataJobNodeCLL.batchSize}") final Integer batchSize,
      @Value("${systemUpdate.dataJobNodeCLL.delayMs}") final Integer delayMs,
      @Value("${systemUpdate.dataJobNodeCLL.limit}") final Integer limit) {
    return new ReindexDataJobViaNodesCLL(
        opContext, entityService, aspectDao, enabled, batchSize, delayMs, limit);
  }
}
