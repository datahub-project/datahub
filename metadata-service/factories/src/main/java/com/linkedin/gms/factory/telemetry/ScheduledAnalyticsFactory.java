/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.telemetry;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.metadata.context.OperationContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Slf4j
@Configuration
@EnableScheduling
public class ScheduledAnalyticsFactory {

  @Bean
  @ConditionalOnProperty("telemetry.enabledServer")
  public DailyReport dailyReport(
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      @Qualifier("searchClientShim") SearchClientShim<?> elasticClient,
      ConfigurationProvider configurationProvider,
      EntityService<?> entityService,
      GitVersion gitVersion) {
    return new DailyReport(
        systemOperationContext, elasticClient, configurationProvider, entityService, gitVersion);
  }
}
