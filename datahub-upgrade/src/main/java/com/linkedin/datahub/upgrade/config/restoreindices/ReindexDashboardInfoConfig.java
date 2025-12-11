/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.config.restoreindices;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.restoreindices.dashboardinfo.ReindexDashboardInfo;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCondition.NonBlockingSystemUpdateCondition.class)
public class ReindexDashboardInfoConfig {

  @Bean
  public NonBlockingSystemUpgrade reindexDashboardInfo(
      final OperationContext opContext,
      final EntityService<?> entityService,
      final AspectDao aspectDao,
      @Value("${systemUpdate.dashboardInfo.enabled}") final boolean enabled,
      @Value("${systemUpdate.dashboardInfo.batchSize}") final Integer batchSize,
      @Value("${systemUpdate.dashboardInfo.delayMs}") final Integer delayMs,
      @Value("${systemUpdate.dashboardInfo.limit}") final Integer limit) {
    return new ReindexDashboardInfo(
        opContext, entityService, aspectDao, enabled, batchSize, delayMs, limit);
  }
}
