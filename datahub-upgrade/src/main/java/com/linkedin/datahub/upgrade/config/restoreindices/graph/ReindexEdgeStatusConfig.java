/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.config.restoreindices.graph;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.restoreindices.graph.edgestatus.ReindexEdgeStatus;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCondition.NonBlockingSystemUpdateCondition.class)
public class ReindexEdgeStatusConfig {

  @Bean
  public NonBlockingSystemUpgrade reindexEdgeStatus(
      final OperationContext opContext,
      final EntityService<?> entityService,
      final AspectDao aspectDao,
      @Value("${elasticsearch.search.graph.graphStatusEnabled}") final boolean featureEnabled,
      @Value("${systemUpdate.edgeStatus.enabled}") final boolean enabled,
      @Value("${systemUpdate.edgeStatus.batchSize}") final Integer batchSize,
      @Value("${systemUpdate.edgeStatus.delayMs}") final Integer delayMs,
      @Value("${systemUpdate.edgeStatus.limit}") final Integer limit) {
    return new ReindexEdgeStatus(
        opContext, entityService, aspectDao, featureEnabled && enabled, batchSize, delayMs, limit);
  }
}
