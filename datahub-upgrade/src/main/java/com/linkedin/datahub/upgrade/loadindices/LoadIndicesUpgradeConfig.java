package com.linkedin.datahub.upgrade.loadindices;

import com.linkedin.datahub.upgrade.config.OpenTelemetryConfig;
import com.linkedin.gms.factory.entity.RetentionBufferFactory;
import com.linkedin.gms.factory.entity.RetentionBufferSchedulingConfig;
import org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration;
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
    // Upgrade CLI is not an ingesting process — keep it out of the post-commit retention buffer +
    // drainer so a short-lived job never competes for the cluster-wide drain lock. Retention (if
    // any
    // ingest happens here) falls back to synchronous post-commit via RetentionBuffer.NO_OP.
    excludeFilters = {
      @ComponentScan.Filter(
          type = FilterType.ASSIGNABLE_TYPE,
          classes = {RetentionBufferFactory.class, RetentionBufferSchedulingConfig.class})
    })
public class LoadIndicesUpgradeConfig {}
