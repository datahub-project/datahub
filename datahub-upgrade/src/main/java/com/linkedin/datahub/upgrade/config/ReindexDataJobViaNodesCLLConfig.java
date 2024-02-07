package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.system.via.ReindexDataJobViaNodesCLL;
import com.linkedin.metadata.entity.EntityService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ReindexDataJobViaNodesCLLConfig {

  @Bean
  public ReindexDataJobViaNodesCLL _reindexDataJobViaNodesCLL(
      EntityService<?> entityService,
      @Value("${systemUpdate.dataJobNodeCLL.enabled}") final boolean enabled,
      @Value("${systemUpdate.dataJobNodeCLL.batchSize}") final Integer batchSize) {
    return new ReindexDataJobViaNodesCLL(entityService, enabled, batchSize);
  }
}
