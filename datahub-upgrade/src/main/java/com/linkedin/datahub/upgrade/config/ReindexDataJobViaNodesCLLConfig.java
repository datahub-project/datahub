package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.vianodes.ReindexDataJobViaNodesCLL;
import com.linkedin.metadata.entity.EntityService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCondition.NonBlockingSystemUpdateCondition.class)
public class ReindexDataJobViaNodesCLLConfig {

  @Bean
  public NonBlockingSystemUpgrade reindexDataJobViaNodesCLL(
      EntityService<?> entityService,
      @Value("${systemUpdate.dataJobNodeCLL.enabled}") final boolean enabled,
      @Value("${systemUpdate.dataJobNodeCLL.batchSize}") final Integer batchSize) {
    return new ReindexDataJobViaNodesCLL(entityService, enabled, batchSize);
  }
}
