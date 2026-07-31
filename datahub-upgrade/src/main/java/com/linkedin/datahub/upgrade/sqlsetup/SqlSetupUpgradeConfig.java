package com.linkedin.datahub.upgrade.sqlsetup;

import com.linkedin.datahub.upgrade.config.OpenTelemetryConfig;
import com.linkedin.gms.factory.common.LocalEbeanConfigFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.system_telemetry.CacheInstrumentationFactory;
import org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration;
import org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration;
import org.springframework.boot.micrometer.metrics.autoconfigure.export.simple.SimpleMetricsExportAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;

/**
 * Configuration for SqlSetup upgrade that excludes Kafka components to prevent connection attempts
 * during database setup.
 *
 * <p>Mirrors the telemetry wiring used by {@code LoadIndicesUpgradeConfig} and {@code
 * CleanupUpgradeConfig} so that {@code datahub-upgrade -u SqlSetup} produces metrics/traces
 * consistent with the rest of system-update. Without this, {@code MicrometerMetricsFactory} (loaded
 * via {@code com.linkedin.gms.factory.system_telemetry}) cannot find a {@link
 * io.datahubproject.metadata.context.SystemTelemetryContext} bean and the {@code otelTracer} bean
 * fails to wire when {@code management.tracing.enabled=true} (the default in {@code
 * application.yaml}).
 *
 * <p>{@link ConfigurationProvider} and {@link LocalEbeanConfigFactory} are imported directly
 * (rather than wholesale-scanning {@code com.linkedin.gms.factory.config} / {@code
 * com.linkedin.gms.factory.common}) to avoid pulling in {@code SystemMetadataServiceFactory}, ES
 * graph factories, and similar heavy components that demand entity/search infrastructure unused by
 * SqlSetup.
 *
 * <p>Spring Boot's metrics auto-configurations are imported explicitly because the SqlSetup
 * application context is bootstrapped via {@link
 * org.springframework.boot.builder.SpringApplicationBuilder} on a plain {@code @Configuration}
 * primary source (no {@code @EnableAutoConfiguration}), so a {@code MeterRegistry} would otherwise
 * be missing for {@code MetricUtilsFactory}.
 */
@Configuration
@Import({
  MetricsAutoConfiguration.class,
  CompositeMeterRegistryAutoConfiguration.class,
  SimpleMetricsExportAutoConfiguration.class,
  ConfigurationProvider.class,
  LocalEbeanConfigFactory.class,
  OpenTelemetryConfig.class
})
@ComponentScan(
    basePackages = {
      "com.linkedin.datahub.upgrade.sqlsetup.config",
      "com.linkedin.gms.factory.entityregistry",
      "com.linkedin.gms.factory.plugins",
      "com.linkedin.gms.factory.system_telemetry"
    },
    excludeFilters =
        @ComponentScan.Filter(
            type = FilterType.ASSIGNABLE_TYPE,
            classes = CacheInstrumentationFactory.class))
public class SqlSetupUpgradeConfig {}
