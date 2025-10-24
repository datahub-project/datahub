package com.linkedin.datahub.upgrade.loadindices;

import com.linkedin.datahub.upgrade.config.OpenTelemetryConfig;
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;

/**
 * Configuration for LoadIndices upgrade that excludes Kafka components to prevent connection
 * attempts during index loading.
 */
@Configuration
@Import({MetricsAutoConfiguration.class, OpenTelemetryConfig.class})
@ComponentScan(
    basePackages = {
      "com.linkedin.datahub.upgrade.loadindices.config",
      "com.linkedin.gms.factory.config",
      "com.linkedin.gms.factory.common",
      "com.linkedin.gms.factory.entity",
      "com.linkedin.gms.factory.entityclient",
      "com.linkedin.gms.factory.plugins",
      "com.linkedin.gms.factory.entityregistry",
      "com.linkedin.gms.factory.search",
      "com.linkedin.gms.factory.timeseries",
      "com.linkedin.gms.factory.context",
      "com.linkedin.gms.factory.system_telemetry"
    },
    excludeFilters = {
      @ComponentScan.Filter(
          type = FilterType.ASSIGNABLE_TYPE,
          classes = {})
    })
public class LoadIndicesUpgradeConfig {}
