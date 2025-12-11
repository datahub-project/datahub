/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.openapi.config;

import io.datahubproject.openapi.delegates.DatahubUsageEventsImpl;
import io.datahubproject.openapi.v2.generated.controller.DatahubUsageEventsApiDelegate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenapiAnalyticsConfig {
  @Bean
  public DatahubUsageEventsApiDelegate datahubUsageEventsApiDelegate() {
    return new DatahubUsageEventsImpl();
  }
}
