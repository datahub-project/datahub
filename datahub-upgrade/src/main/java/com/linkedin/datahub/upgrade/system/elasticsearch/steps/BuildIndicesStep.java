package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import com.google.common.base.Throwables;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.*;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.utils.elasticsearch.TaskFailureParseResult;
import com.linkedin.metadata.utils.elasticsearch.TaskFailureParser;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BuildIndicesStep implements UpgradeStep {

  private final List<ElasticSearchIndexed> services;
  private final Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties;
  private final ConfigurationProvider configurationProvider;

  // Full constructor with ConfigurationProvider (for BuildIndices)
  public BuildIndicesStep(
      List<ElasticSearchIndexed> services,
      Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties,
      ConfigurationProvider configurationProvider) {
    this.services = services;
    this.structuredProperties = structuredProperties;
    this.configurationProvider = configurationProvider;
  }

  // Backward-compatible constructor without ConfigurationProvider (for LoadIndices)
  public BuildIndicesStep(
      List<ElasticSearchIndexed> services,
      Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties) {
    this.services = services;
    this.structuredProperties = structuredProperties;
    this.configurationProvider = null;
  }

  @Override
  public String id() {
    return "BuildIndicesStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        // If no configuration provider, use sequential reindexing
        if (configurationProvider == null) {
          log.info("No configuration provider available, using sequential reindexing");
          return executeSequentialReindex(context);
        }

        BuildIndicesConfiguration config =
            configurationProvider.getElasticSearch().getBuildIndices();

        if (config != null && config.isEnableParallelReindex()) {
          log.info("Parallel reindexing enabled");
          return executeParallelReindex(context, config);
        } else {
          log.info("Using sequential reindexing");
          return executeSequentialReindex(context);
        }
      } catch (Exception e) {
        Throwable root = Throwables.getRootCause(e);
        log.error(
            "BuildIndicesStep failed. Root cause: [{}] {}",
            root.getClass().getSimpleName(),
            root.getMessage(),
            e);
        context
            .report()
            .addLine(
                String.format(
                    "%s failed. Root cause: [%s] %s",
                    id(), root.getClass().getSimpleName(), root.getMessage()));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private UpgradeStepResult executeSequentialReindex(UpgradeContext context) throws Exception {
    for (ElasticSearchIndexed service : services) {
      service.reindexAll(context.opContext(), structuredProperties);
    }
    return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
  }

  private UpgradeStepResult executeParallelReindex(
      UpgradeContext context, BuildIndicesConfiguration config) throws Exception {
    // Collect all reindex configs from all services
    List<ReindexConfig> allConfigs = new ArrayList<>();
    for (ElasticSearchIndexed service : services) {
      List<ReindexConfig> serviceConfigs =
          service.buildReindexConfigs(context.opContext(), structuredProperties);
      allConfigs.addAll(serviceConfigs);
    }

    log.info(
        "Collected {} total reindex configs across {} services",
        allConfigs.size(),
        services.size());
    // Use the first service's index builder (they all use the same ES cluster)
    if (services.isEmpty() || allConfigs.isEmpty()) {
      log.info("No services or configs to reindex");
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    }

    // IMPORTANT: Process non-reindex configs first (new indices, settings-only changes)
    // to ensure all indices exist before parallel reindexing starts
    List<ReindexConfig> nonReindexConfigs =
        allConfigs.stream().filter(c -> !c.requiresReindex()).toList();
    List<ReindexConfig> reindexConfigs =
        allConfigs.stream().filter(ReindexConfig::requiresReindex).toList();

    log.info(
        "Processing {} non-reindex configs (new indices, settings changes)",
        nonReindexConfigs.size());
    Map<String, ReindexResult> results = new HashMap<>();
    for (ReindexConfig nonReindexConfig : nonReindexConfigs) {
      results.put(
          nonReindexConfig.name(),
          services.get(0).getIndexBuilder().buildIndex(context.opContext(), nonReindexConfig));
    }

    // Only use parallel orchestrator for configs that actually need reindexing
    if (reindexConfigs.isEmpty()) {
      log.info("No indices require reindexing");
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    }

    log.info("Starting parallel reindex for {} indices", reindexConfigs.size());

    // Create shared circuit breaker state for both large and normal indices
    CircuitBreakerState circuitBreakerState = new CircuitBreakerState(config);
    // Start HealthCheckPoller using ScheduledExecutorService to continuously monitor cluster health
    // This daemon thread updates CircuitBreakerState, which is shared by all reindex operations
    ScheduledExecutorService healthCheckExecutor =
        Executors.newScheduledThreadPool(
            1,
            r -> {
              Thread t = new Thread(r, "HealthCheckPoller");
              t.setDaemon(true);
              return t;
            });
    HealthCheckPoller healthPoller =
        new HealthCheckPoller(
            context.opContext(),
            services.get(0).getIndexBuilder(),
            circuitBreakerState,
            config.getClusterHeapThresholdPercent(),
            config.getClusterHeapYellowThresholdPercent(),
            config.getWriteRejectionRedThreshold());
    // Schedule health polling at fixed intervals
    healthCheckExecutor.scheduleAtFixedRate(
        healthPoller::poll, 0, config.getClusterHealthCheckIntervalSeconds(), TimeUnit.SECONDS);
    log.info(
        "HealthCheckPoller scheduled with {} second interval",
        config.getClusterHealthCheckIntervalSeconds());
    ParallelReindexOrchestrator orchestrator = null;
    try {
      orchestrator =
          new ParallelReindexOrchestrator(
              context.opContext(), services.get(0).getIndexBuilder(), config, circuitBreakerState);
      results.putAll(orchestrator.reindexAll(reindexConfigs));
      var documentFailures = orchestrator.getDocumentFailuresByIndex();
      // Check results for hard failures and soft REINDEXED_WITH_FAILURES
      Map<String, ReindexResult> failures =
          results.entrySet().stream()
              .filter(
                  (entry) ->
                      entry.getValue().isFailure()
                          || entry.getValue() == ReindexResult.REINDEXED_WITH_FAILURES)
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      if (!failures.isEmpty()) {
        log.error(
            "Parallel reindex completed with {} failures out of {} indices",
            failures.size(),
            results.size());
        failures.forEach(
            (key, value) -> {
              TaskFailureParseResult indexFailures =
                  documentFailures.getOrDefault(key, TaskFailureParseResult.EMPTY);
              log.error("Failure index alias {} reason :{}", key, value);
              TaskFailureParser.logFailures(
                  log, "Document failures for index " + key, indexFailures);
              String formatted = TaskFailureParser.formatForLog(indexFailures, 5);
              if (value.isFailure()) {
                context
                    .report()
                    .addLine(
                        String.format(
                            "%s failed: Failure index alias %s reason: %s%s",
                            id(), key, value, formatted.isEmpty() ? "" : " " + formatted));
              } else if (value == ReindexResult.REINDEXED_WITH_FAILURES) {
                context
                    .report()
                    .addLine(
                        String.format(
                            "%s: index %s reindexed with document failures%s",
                            id(), key, formatted.isEmpty() ? "" : ": " + formatted));
                log.warn("Index {} completed as REINDEXED_WITH_FAILURES (soft-pass)", key);
              }
            });
        boolean hardFailure = failures.values().stream().anyMatch(ReindexResult::isFailure);
        if (hardFailure) {
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }
      }
      log.info("Parallel reindex completed successfully for {} indices", results.size());
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    } finally {
      // Shutdown the health check executor gracefully
      log.info("Shutting down HealthCheckPoller executor");
      try {
        healthCheckExecutor.shutdown();
        if (!healthCheckExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
          healthCheckExecutor.shutdownNow();
          log.warn("HealthCheckPoller executor did not shut down cleanly, forced shutdown");
        } else {
          log.info("HealthCheckPoller executor stopped gracefully");
        }
      } catch (InterruptedException e) {
        healthCheckExecutor.shutdownNow();
        log.error("Interrupted while shutting down HealthCheckPoller executor", e);
      }
      if (orchestrator != null) {
        orchestrator.shutdown();
      }
    }
  }
}
