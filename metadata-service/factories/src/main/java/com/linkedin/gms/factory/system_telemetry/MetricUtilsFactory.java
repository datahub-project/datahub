/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.system_telemetry;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricUtilsFactory {
  @Bean
  public MetricUtils metricUtils(
      MeterRegistry meterRegistry, ConfigurationProvider configurationProvider) {
    return MetricUtils.builder().registry(meterRegistry).build();
  }
}
