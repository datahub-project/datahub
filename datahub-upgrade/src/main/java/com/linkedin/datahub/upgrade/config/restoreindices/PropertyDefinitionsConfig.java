package com.linkedin.datahub.upgrade.config.restoreindices;

import com.linkedin.datahub.upgrade.config.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.restoreindices.structuredproperties.PropertyDefinitions;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCondition.NonBlockingSystemUpdateCondition.class)
public class PropertyDefinitionsConfig {

  @Bean
  public NonBlockingSystemUpgrade propertyDefinitions(
      final OperationContext opContext,
      final EntityService<?> entityService,
      final AspectDao aspectDao,
      @Value("${systemUpdate.propertyDefinitions.enabled}") final boolean enabled,
      @Value("${systemUpdate.propertyDefinitions.batchSize}") final Integer batchSize,
      @Value("${systemUpdate.propertyDefinitions.delayMs}") final Integer delayMs,
      @Value("${systemUpdate.propertyDefinitions.limit}") final Integer limit) {
    return new PropertyDefinitions(
        opContext, entityService, aspectDao, enabled, batchSize, delayMs, limit);
  }
}
