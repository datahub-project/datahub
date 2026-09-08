package com.linkedin.gms.factory.analytics;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;

/**
 * Rejects mixed pgAnalytics enablement: schema/pool on while product usage events still use search,
 * or postgres usage SoT without {@code DATAHUB_PGANALYTICS_ENABLED}. Dual-write is not supported.
 *
 * <p>SqlSetup can still create tables from {@code enabled} alone; this guard is loaded by GMS / MAE
 * / MCE, not the SqlSetup upgrade job.
 */
@Configuration
@RequiredArgsConstructor
public class PgAnalyticsBackendGuard {

  private final ConfigurationProvider configurationProvider;
  private final PostgresSqlSetupProperties postgresSqlSetupProperties;

  @PostConstruct
  void validateExclusiveSourceOfTruth() {
    validate(
        postgresSqlSetupProperties.getPgAnalytics().isEnabled(),
        configurationProvider.getPlatformAnalytics().getUsageEvents().usePostgresql());
  }

  static void validate(boolean enabled, boolean postgresUsageEvents) {
    if (enabled && !postgresUsageEvents) {
      throw new IllegalStateException(
          "postgres.pgAnalytics.enabled=true requires"
              + " platformAnalytics.usage-events.implementation=postgres; dual-write is not"
              + " supported");
    }
    if (postgresUsageEvents && !enabled) {
      throw new IllegalStateException(
          "platformAnalytics.usage-events.implementation=postgres requires"
              + " postgres.pgAnalytics.enabled=true (DATAHUB_PGANALYTICS_ENABLED) and SqlSetup.");
    }
  }
}
