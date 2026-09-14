package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.*;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
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
        log.error("BuildIndicesStep failed.", e);
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
    List<ReindexConfig> allConfigs =
        IndexUtils.getAllReindexConfigs(context.opContext(), services, structuredProperties);

    log.info(
        "Collected {} total reindex configs across {} services",
        allConfigs.size(),
        services.size());
    if (allConfigs.isEmpty()) {
      log.info("No services or configs to reindex");
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    }

    IdentityHashMap<SearchClientShim<?>, ClusterBatch> byCluster = new IdentityHashMap<>();
    for (ReindexConfig indexConfig : allConfigs) {
      ESIndexBuilder builder = IndexUtils.requireIndexBuilder(indexConfig.name());
      byCluster
          .computeIfAbsent(builder.getSearchClient(), ignored -> new ClusterBatch(builder))
          .configs
          .add(indexConfig);
    }

    Map<String, ReindexResult> results = new HashMap<>();
    for (ClusterBatch batch : byCluster.values()) {
      results.putAll(reindexClusterBatch(context, config, batch));
    }

    Map<String, ReindexResult> failures =
        results.entrySet().stream()
            .filter((key) -> key.getValue().isFailure())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    if (!failures.isEmpty()) {
      log.error(
          "Parallel reindex completed with {} failures out of {} indices",
          failures.size(),
          results.size());
      failures.forEach((key, value) -> log.error("Failure index alias {} reason :{}", key, value));
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
    }
    log.info("Parallel reindex completed successfully for {} indices", results.size());
    return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
  }

  private Map<String, ReindexResult> reindexClusterBatch(
      UpgradeContext context, BuildIndicesConfiguration config, ClusterBatch batch)
      throws Exception {
    List<ReindexConfig> nonReindexConfigs =
        batch.configs.stream().filter(c -> !c.requiresReindex()).toList();
    List<ReindexConfig> reindexConfigs =
        batch.configs.stream().filter(ReindexConfig::requiresReindex).toList();

    log.info(
        "Cluster {}: {} non-reindex configs, {} reindex configs",
        batch.builder.getSearchClient(),
        nonReindexConfigs.size(),
        reindexConfigs.size());

    Map<String, ReindexResult> results = new HashMap<>();
    for (ReindexConfig nonReindexConfig : nonReindexConfigs) {
      results.put(
          nonReindexConfig.name(), batch.builder.buildIndex(context.opContext(), nonReindexConfig));
    }

    if (reindexConfigs.isEmpty()) {
      return results;
    }

    CircuitBreakerState circuitBreakerState = new CircuitBreakerState(config);
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
            batch.builder,
            circuitBreakerState,
            config.getClusterHeapThresholdPercent(),
            config.getClusterHeapYellowThresholdPercent(),
            config.getWriteRejectionRedThreshold());
    healthCheckExecutor.scheduleAtFixedRate(
        healthPoller::poll, 0, config.getClusterHealthCheckIntervalSeconds(), TimeUnit.SECONDS);
    ParallelReindexOrchestrator orchestrator = null;
    try {
      orchestrator =
          new ParallelReindexOrchestrator(
              context.opContext(), batch.builder, config, circuitBreakerState);
      results.putAll(orchestrator.reindexAll(reindexConfigs));
      return results;
    } finally {
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

  private static final class ClusterBatch {
    private final ESIndexBuilder builder;
    private final List<ReindexConfig> configs = new ArrayList<>();

    private ClusterBatch(ESIndexBuilder builder) {
      this.builder = builder;
    }
  }
}
