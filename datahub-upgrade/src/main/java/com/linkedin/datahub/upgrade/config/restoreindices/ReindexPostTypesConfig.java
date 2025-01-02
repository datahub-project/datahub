package com.linkedin.datahub.upgrade.config.restoreindices;

import com.linkedin.datahub.upgrade.config.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.restoreindices.posts.ReindexPostTypes;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCondition.NonBlockingSystemUpdateCondition.class)
public class ReindexPostTypesConfig {

  @Bean
  public NonBlockingSystemUpgrade reindexPostInfo(
      final OperationContext opContext,
      final EntityService<?> entityService,
      final AspectDao aspectDao,
      @Value("${systemUpdate.postInfo.enabled}") final boolean enabled,
      @Value("${systemUpdate.postInfo.batchSize}") final Integer batchSize,
      @Value("${systemUpdate.postInfo.delayMs}") final Integer delayMs,
      @Value("${systemUpdate.postInfo.limit}") final Integer limit) {
    return new ReindexPostTypes(
        opContext, entityService, aspectDao, enabled, batchSize, delayMs, limit);
  }
}
