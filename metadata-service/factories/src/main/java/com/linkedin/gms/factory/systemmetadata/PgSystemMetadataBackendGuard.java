package com.linkedin.gms.factory.systemmetadata;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;

/**
 * Rejects mixed pgSystemMetadata enablement. Dual-write is not supported. SqlSetup may still create
 * tables from {@code enabled} alone; this guard is loaded by GMS / MAE, not the SqlSetup job.
 */
@Configuration
@RequiredArgsConstructor
public class PgSystemMetadataBackendGuard {

  private final ConfigurationProvider configurationProvider;
  private final PostgresSqlSetupProperties postgresSqlSetupProperties;

  @PostConstruct
  void validateExclusiveSourceOfTruth() {
    validate(
        postgresSqlSetupProperties.getPgSystemMetadata().isEnabled(),
        configurationProvider.getSystemMetadataService().getImplementation() != null
            && "postgres"
                .equalsIgnoreCase(
                    configurationProvider.getSystemMetadataService().getImplementation().name()));
  }

  static void validate(boolean enabled, boolean postgresImplementation) {
    if (enabled && !postgresImplementation) {
      throw new IllegalStateException(
          "postgres.pgSystemMetadata.enabled=true requires"
              + " systemMetadataService.implementation=postgres; dual-write is not supported");
    }
    if (postgresImplementation && !enabled) {
      throw new IllegalStateException(
          "systemMetadataService.implementation=postgres requires"
              + " postgres.pgSystemMetadata.enabled=true (DATAHUB_PGSYSTEMMETADATA_ENABLED) and"
              + " SqlSetup.");
    }
  }
}
