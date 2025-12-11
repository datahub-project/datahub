/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.kafka;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.system_telemetry.OpenTelemetryBaseFactory;
import com.linkedin.metadata.event.GenericProducer;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MCEOpenTelemetryConfig extends OpenTelemetryBaseFactory {

  @Override
  protected String getApplicationComponent() {
    return "datahub-mce-consumer";
  }

  @Bean
  @Override
  protected SystemTelemetryContext traceContext(
      MetricUtils metricUtils,
      ConfigurationProvider configurationProvider,
      @Qualifier("dataHubUsageEventProducer") GenericProducer<String> dueProducer) {
    return super.traceContext(metricUtils, configurationProvider, dueProducer);
  }
}
