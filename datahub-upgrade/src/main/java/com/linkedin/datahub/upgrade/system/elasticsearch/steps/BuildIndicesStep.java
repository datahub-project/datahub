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
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
        } else if (config != null) {
          log.info("Applying non-reindex settings in parallel, then reindexing sequentially");
          return executeParallelSettingsThenSequentialReindex(context, config);
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

  private UpgradeStepResult executeParallelSettingsThenSequentialReindex(
      UpgradeContext context, BuildIndicesConfiguration config) throws Exception {
    List<ReindexConfig> allConfigs =
        IndexUtils.getAllReindexConfigs(context.opContext(), services, structuredProperties);
    if (allConfigs.isEmpty()) {
      log.info("No services or configs to reindex");
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    }
    List<Pair<ESIndexBuilder, ReindexConfig>> nonReindex = new ArrayList<>();
    List<Pair<ESIndexBuilder, ReindexConfig>> reindex = new ArrayList<>();
    for (ReindexConfig indexConfig : allConfigs) {
      ESIndexBuilder builder = IndexUtils.requireIndexBuilder(indexConfig.name());
      if (indexConfig.requiresReindex()) {
        reindex.add(Pair.of(builder, indexConfig));
      } else {
        nonReindex.add(Pair.of(builder, indexConfig));
      }
    }
    Map<String, ReindexResult> results =
        applyNonReindexParallel(context, nonReindex, requireSettingsPoolSize(config));
    for (Pair<ESIndexBuilder, ReindexConfig> item : reindex) {
      results.put(
          item.getSecond().name(),
          item.getFirst().buildIndex(context.opContext(), item.getSecond()));
    }
    return resultFrom(results);
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

    // Outer: HTTP cluster identity. Inner: overlay-specific builder. Non-reindex settings
    // updates share one pool per client. Reindex stays on that client's orchestrator.
    IdentityHashMap<SearchClientShim<?>, IdentityHashMap<ESIndexBuilder, ClusterBatch>> byClient =
        new IdentityHashMap<>();
    for (ReindexConfig indexConfig : allConfigs) {
      ESIndexBuilder builder = IndexUtils.requireIndexBuilder(indexConfig.name());
      byClient
          .computeIfAbsent(builder.getSearchClient(), ignored -> new IdentityHashMap<>())
          .computeIfAbsent(builder, ClusterBatch::new)
          .configs
          .add(indexConfig);
    }

    log.info("Parallel reindex grouped into {} unique search clients", byClient.size());

    AtomicInteger clusterThread = new AtomicInteger();
    ExecutorService clusterPool =
        Executors.newFixedThreadPool(
            byClient.size(),
            r -> {
              Thread t = new Thread(r, "BuildIndices-cluster-" + clusterThread.incrementAndGet());
              t.setDaemon(true);
              return t;
            });
    List<Future<Map<String, ReindexResult>>> futures = new ArrayList<>();
    try {
      for (Map.Entry<SearchClientShim<?>, IdentityHashMap<ESIndexBuilder, ClusterBatch>> entry :
          byClient.entrySet()) {
        SearchClientShim<?> client = entry.getKey();
        Collection<ClusterBatch> batches = entry.getValue().values();
        futures.add(
            clusterPool.submit(() -> reindexClientBatches(context, config, client, batches)));
      }

      Map<String, ReindexResult> results = new HashMap<>();
      boolean clusterFailed = false;
      for (Future<Map<String, ReindexResult>> future : futures) {
        try {
          results.putAll(future.get());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          log.error("Interrupted while waiting for a cluster reindex batch", e);
          clusterFailed = true;
        } catch (ExecutionException e) {
          log.error("Cluster reindex batch failed", e.getCause() != null ? e.getCause() : e);
          clusterFailed = true;
        }
      }

      Map<String, ReindexResult> failures =
          results.entrySet().stream()
              .filter((key) -> key.getValue().isFailure())
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      if (clusterFailed || !failures.isEmpty()) {
        log.error(
            "Parallel reindex completed with {} failures out of {} indices{}",
            failures.size(),
            results.size(),
            clusterFailed ? " and at least one cluster-level error" : "");
        failures.forEach(
            (key, value) -> log.error("Failure index alias {} reason :{}", key, value));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
      log.info("Parallel reindex completed successfully for {} indices", results.size());
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    } finally {
      clusterPool.shutdown();
      try {
        if (!clusterPool.awaitTermination(10, TimeUnit.SECONDS)) {
          clusterPool.shutdownNow();
        }
      } catch (InterruptedException e) {
        clusterPool.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  private Map<String, ReindexResult> reindexClientBatches(
      UpgradeContext context,
      BuildIndicesConfiguration config,
      SearchClientShim<?> client,
      Collection<ClusterBatch> batches)
      throws Exception {
    boolean needsHealthPoller =
        batches.stream()
            .flatMap(batch -> batch.configs.stream())
            .anyMatch(ReindexConfig::requiresReindex);

    CircuitBreakerState circuitBreakerState =
        needsHealthPoller ? new CircuitBreakerState(config) : null;
    ScheduledExecutorService healthCheckExecutor = null;
    try {
      if (needsHealthPoller) {
        ESIndexBuilder pollerBuilder =
            batches.stream()
                .filter(batch -> batch.configs.stream().anyMatch(ReindexConfig::requiresReindex))
                .map(batch -> batch.builder)
                .findFirst()
                .orElseThrow();
        int clientId = System.identityHashCode(client);
        healthCheckExecutor =
            Executors.newScheduledThreadPool(
                1,
                r -> {
                  Thread t = new Thread(r, "HealthCheckPoller-" + clientId);
                  t.setDaemon(true);
                  return t;
                });
        HealthCheckPoller healthPoller =
            new HealthCheckPoller(
                context.opContext(),
                pollerBuilder,
                circuitBreakerState,
                config.getClusterHeapThresholdPercent(),
                config.getClusterHeapYellowThresholdPercent(),
                config.getWriteRejectionRedThreshold());
        healthCheckExecutor.scheduleAtFixedRate(
            healthPoller::poll, 0, config.getClusterHealthCheckIntervalSeconds(), TimeUnit.SECONDS);
      }

      Map<String, ReindexResult> results = new HashMap<>();
      List<Pair<ESIndexBuilder, ReindexConfig>> nonReindex = new ArrayList<>();
      List<ClusterBatch> reindexBatches = new ArrayList<>();
      for (ClusterBatch batch : batches) {
        ClusterBatch reindexBatch = null;
        for (ReindexConfig indexConfig : batch.configs) {
          if (indexConfig.requiresReindex()) {
            if (reindexBatch == null) {
              reindexBatch = new ClusterBatch(batch.builder);
              reindexBatches.add(reindexBatch);
            }
            reindexBatch.configs.add(indexConfig);
          } else {
            nonReindex.add(Pair.of(batch.builder, indexConfig));
          }
        }
      }
      results.putAll(applyNonReindexParallel(context, nonReindex, requireSettingsPoolSize(config)));
      for (ClusterBatch batch : reindexBatches) {
        results.putAll(reindexBuilderBatch(context, config, batch, circuitBreakerState));
      }
      return results;
    } finally {
      shutdownHealthCheckExecutor(healthCheckExecutor);
    }
  }

  private Map<String, ReindexResult> reindexBuilderBatch(
      UpgradeContext context,
      BuildIndicesConfiguration config,
      ClusterBatch batch,
      CircuitBreakerState circuitBreakerState)
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
    if (reindexConfigs.isEmpty()) {
      return results;
    }

    ParallelReindexOrchestrator orchestrator = null;
    try {
      orchestrator =
          new ParallelReindexOrchestrator(
              context.opContext(), batch.builder, config, circuitBreakerState);
      results.putAll(orchestrator.reindexAll(reindexConfigs));
      return results;
    } finally {
      if (orchestrator != null) {
        orchestrator.shutdown();
      }
    }
  }

  private Map<String, ReindexResult> applyNonReindexParallel(
      UpgradeContext context, List<Pair<ESIndexBuilder, ReindexConfig>> work, int poolSize)
      throws InterruptedException {
    if (work.isEmpty()) {
      return new HashMap<>();
    }
    log.info("Applying settings for {} indices with pool size {}", work.size(), poolSize);
    int threads = Math.min(poolSize, work.size());
    AtomicInteger thread = new AtomicInteger();
    ExecutorService pool =
        Executors.newFixedThreadPool(
            threads,
            runnable -> {
              Thread settingsThread =
                  new Thread(runnable, "BuildIndices-settings-" + thread.incrementAndGet());
              settingsThread.setDaemon(true);
              return settingsThread;
            });
    List<Future<Pair<String, ReindexResult>>> futures = new ArrayList<>();
    try {
      for (Pair<ESIndexBuilder, ReindexConfig> item : work) {
        futures.add(pool.submit(() -> applyOneIndex(context, item.getFirst(), item.getSecond())));
      }
      Map<String, ReindexResult> results = new HashMap<>();
      for (Future<Pair<String, ReindexResult>> future : futures) {
        Pair<String, ReindexResult> result = future.get();
        results.put(result.getKey(), result.getValue());
      }
      return results;
    } catch (InterruptedException e) {
      futures.forEach(future -> future.cancel(true));
      pool.shutdownNow();
      Thread.currentThread().interrupt();
      throw e;
    } catch (ExecutionException e) {
      throw new IllegalStateException("Settings update failed", e.getCause());
    } finally {
      if (!pool.isShutdown()) {
        pool.shutdown();
      }
      try {
        if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
          pool.shutdownNow();
        }
      } catch (InterruptedException e) {
        pool.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  private Pair<String, ReindexResult> applyOneIndex(
      UpgradeContext context, ESIndexBuilder builder, ReindexConfig indexConfig) {
    try {
      return Pair.of(indexConfig.name(), builder.buildIndex(context.opContext(), indexConfig));
    } catch (Exception e) {
      log.error("Failed to apply index settings for {}", indexConfig.name(), e);
      return Pair.of(indexConfig.name(), ReindexResult.FAILED_SUBMISSION);
    }
  }

  private static int requireSettingsPoolSize(BuildIndicesConfiguration config) {
    Integer poolSize = config.getMaxConcurrentSettingsUpdates();
    if (poolSize == null || poolSize <= 0) {
      throw new IllegalStateException(
          "elasticsearch.buildIndices.maxConcurrentSettingsUpdates must be a positive integer");
    }
    return poolSize;
  }

  private UpgradeStepResult resultFrom(Map<String, ReindexResult> results) {
    Map<String, ReindexResult> failures =
        results.entrySet().stream()
            .filter(entry -> entry.getValue().isFailure())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    if (!failures.isEmpty()) {
      log.error(
          "BuildIndices completed with {} failures out of {} indices",
          failures.size(),
          results.size());
      failures.forEach((key, value) -> log.error("Failure index {} reason {}", key, value));
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
    }
    return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
  }

  private static void shutdownHealthCheckExecutor(ScheduledExecutorService healthCheckExecutor) {
    if (healthCheckExecutor == null) {
      return;
    }
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
      Thread.currentThread().interrupt();
      log.error("Interrupted while shutting down HealthCheckPoller executor", e);
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
