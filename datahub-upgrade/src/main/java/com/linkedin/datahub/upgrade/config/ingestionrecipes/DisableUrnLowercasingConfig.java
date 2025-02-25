package com.linkedin.datahub.upgrade.config.ingestionrecipes;

import com.linkedin.datahub.upgrade.config.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.ingestionrecipes.DisableUrnLowercasing;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCondition.NonBlockingSystemUpdateCondition.class)
public class DisableUrnLowercasingConfig {

  @Bean
  public NonBlockingSystemUpgrade disableUrnLowercasing(
      final OperationContext opContext,
      final EntityService<?> entityService,
      @Value("${systemUpdate.disableUrnLowercasing.enabled}") final boolean enabled) {
    return new DisableUrnLowercasing(opContext, entityService, enabled);
  }
}
