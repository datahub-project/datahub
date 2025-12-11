/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.sqlsetup;

import com.linkedin.gms.factory.common.LocalEbeanConfigFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;

/**
 * Configuration for SqlSetup upgrade that excludes Kafka components to prevent connection attempts
 * during database setup.
 */
@Configuration
@Import({
  MetricsAutoConfiguration.class,
  ConfigurationProvider.class,
  LocalEbeanConfigFactory.class
})
@ComponentScan(
    basePackages = {
      "com.linkedin.datahub.upgrade.sqlsetup.config",
      "com.linkedin.gms.factory.entityregistry",
      "com.linkedin.gms.factory.plugins",
    },
    excludeFilters = {
      @ComponentScan.Filter(
          type = FilterType.ASSIGNABLE_TYPE,
          classes = {})
    })
public class SqlSetupUpgradeConfig {}
