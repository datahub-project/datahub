package com.linkedin.gms.factory.graph;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;

/**
 * Rejects mixed pgGraph enablement. Dual-write is not supported. SqlSetup may still create tables
 * from {@code enabled} alone; this guard is loaded by GMS / MAE, not the SqlSetup job.
 */
@Configuration
@RequiredArgsConstructor
public class PgGraphBackendGuard {

  private final ConfigurationProvider configurationProvider;
  private final PostgresSqlSetupProperties postgresSqlSetupProperties;

  @PostConstruct
  void validateExclusiveSourceOfTruth() {
    String type =
        configurationProvider.getGraphService() != null
            ? configurationProvider.getGraphService().getType()
            : null;
    validate(
        postgresSqlSetupProperties.getPgGraph().isEnabled(),
        GraphPostgresBackendCondition.usePostgresGraphService(type));
  }

  static void validate(boolean enabled, boolean postgresImplementation) {
    if (enabled && !postgresImplementation) {
      throw new IllegalStateException(
          "postgres.pgGraph.enabled=true requires graphService.type=postgres"
              + " (GRAPH_SERVICE_IMPL=postgres); dual-write is not supported");
    }
    if (postgresImplementation && !enabled) {
      throw new IllegalStateException(
          "graphService.type=postgres requires postgres.pgGraph.enabled=true"
              + " (DATAHUB_PGGRAPH_ENABLED) and SqlSetup.");
    }
  }
}
