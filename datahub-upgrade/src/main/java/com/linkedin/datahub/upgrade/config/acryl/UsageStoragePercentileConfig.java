package com.linkedin.datahub.upgrade.config.acryl;

import com.linkedin.datahub.upgrade.config.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.acryl.UsageStoragePercentile;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCondition.NonBlockingSystemUpdateCondition.class)
public class UsageStoragePercentileConfig {

  @Bean
  public NonBlockingSystemUpgrade usageStoragePercentile(
      @Qualifier("systemOperationContext") final OperationContext opContext,
      final EntityService<?> entityService,
      final AspectDao aspectDao,
      // SYSTEM_UPDATE_USAGE_STORAGE_PERCENTILE_ENABLED
      @Value("${systemUpdate.usageStoragePercentile.enabled:true}") final boolean enabled,
      // SYSTEM_UPDATE_USAGE_STORAGE_PERCENTILE_BATCH_SIZE
      @Value("${systemUpdate.usageStoragePercentile.batchSize:500}") final Integer batchSize,
      // SYSTEM_UPDATE_USAGE_STORAGE_PERCENTILE_DELAY_MS
      @Value("${systemUpdate.usageStoragePercentile.delayMs:1000}") final Integer delayMs,
      // SYSTEM_UPDATE_USAGE_STORAGE_PERCENTILE_LIMIT
      @Value("${systemUpdate.usageStoragePercentile.limit:0}") final Integer limit) {
    return new UsageStoragePercentile(
        opContext, entityService, aspectDao, enabled, batchSize, delayMs, limit);
  }
}
