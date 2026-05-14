package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.criterion.CriterionFilterAspectsBlocking;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

@Configuration
@Conditional(SystemUpdateCondition.BlockingSystemUpdateCondition.class)
public class CriterionFilterAspectsBlockingConfig {

  /** After {@link CopyDocumentsToSemanticIndicesConfig} (order 3). */
  @Order(4)
  @Bean
  public BlockingSystemUpgrade criterionFilterAspectsBlocking(
      @Qualifier("systemOperationContext") final OperationContext opContext,
      final EntityService<?> entityService,
      final AspectDao aspectDao,
      final GitVersion gitVersion,
      @Qualifier("revision") final String revision,
      @Value("${systemUpdate.criterionFilterAspectsBlocking.enabled:false}") final boolean enabled,
      @Value("${systemUpdate.criterionFilterAspectsBlocking.batchSize:500}") final int batchSize,
      @Value("${systemUpdate.criterionFilterAspectsBlocking.delayMs:1000}") final int delayMs,
      @Value("${systemUpdate.criterionFilterAspectsBlocking.limit:0}") final int limit) {
    String upgradeVersion =
        String.format("criterion-filter-schema-v2-%s-%s", gitVersion.getVersion(), revision);
    return new CriterionFilterAspectsBlocking(
        opContext, entityService, aspectDao, upgradeVersion, enabled, batchSize, delayMs, limit);
  }
}
