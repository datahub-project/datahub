package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.ownershiptypes.OwnershipTypes;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCondition.NonBlockingSystemUpdateCondition.class)
public class BackfillOwnershipTypesConfig {

  @Bean
  public NonBlockingSystemUpgrade backfillOwnershipTypes(
      final OperationContext opContext,
      final EntityService<?> entityService,
      final SearchService searchService,
      @Value("${systemUpdate.ownershipTypes.enabled}") final boolean enabled,
      @Value("${systemUpdate.ownershipTypes.reprocess.enabled}") final boolean reprocessEnabled,
      @Value("${systemUpdate.ownershipTypes.batchSize}") final Integer batchSize) {
    return new OwnershipTypes(
        opContext, entityService, searchService, enabled, reprocessEnabled, batchSize);
  }
}
