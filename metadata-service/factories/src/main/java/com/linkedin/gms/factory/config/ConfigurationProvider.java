/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.config;

import com.datahub.authentication.AuthenticationConfiguration;
import com.datahub.authorization.AuthorizationConfiguration;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import com.linkedin.metadata.config.GMSConfiguration;
import com.linkedin.metadata.config.PlatformAnalyticsConfiguration;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@EqualsAndHashCode(callSuper = true)
@Component
// Include extra kafka properties
@EnableConfigurationProperties(KafkaProperties.class)
@ConfigurationProperties
@Data
public class ConfigurationProvider extends DataHubAppConfiguration {
  /** Authentication related configs */
  private AuthenticationConfiguration authentication;

  /** Authorizer related configs */
  private AuthorizationConfiguration authorization;

  /** Configuration for the health check server */
  private HealthCheckConfiguration healthCheck;

  /** Enable/disable DataHub analytics */
  private PlatformAnalyticsConfiguration platformAnalytics;

  /**
   * Provides GMSConfiguration bean for dependency injection. This extracts the GMS configuration
   * from the main DataHub configuration.
   */
  @Bean("gmsConfiguration")
  public GMSConfiguration gmsConfiguration() {
    return this.getDatahub().getGms();
  }
}
