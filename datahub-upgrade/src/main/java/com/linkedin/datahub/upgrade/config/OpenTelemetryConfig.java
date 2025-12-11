/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.config;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.system_telemetry.OpenTelemetryBaseFactory;
import com.linkedin.metadata.event.GenericProducer;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import javax.annotation.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenTelemetryConfig extends OpenTelemetryBaseFactory {

  @Override
  protected String getApplicationComponent() {
    return "datahub-upgrade";
  }

  @Bean
  @Override
  protected SystemTelemetryContext traceContext(
      MetricUtils metricUtils,
      ConfigurationProvider configurationProvider,
      @Autowired(required = false) @Qualifier("dataHubUsageEventProducer") @Nullable
          GenericProducer<String> dueProducer) {
    return super.traceContext(metricUtils, configurationProvider, dueProducer);
  }
}
