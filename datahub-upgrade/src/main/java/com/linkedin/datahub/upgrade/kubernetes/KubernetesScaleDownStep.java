package com.linkedin.datahub.upgrade.kubernetes;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.config.kubernetes.DeploymentEnvUpdate;
import com.linkedin.metadata.config.kubernetes.KubernetesScaleDownConfiguration;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Blocking upgrade step that scales down GMS/MAE/MCE in Kubernetes before other upgrade steps run.
 * Persists retry count and last-known restore values in a ConfigMap only—actual cluster state is
 * always read from the Kubernetes API when capturing or restoring.
 */
@Slf4j
public class KubernetesScaleDownStep implements UpgradeStep {

  private static final String STEP_ID = "KubernetesScaleDownStep";

  /**
   * Upper bound on parallel pool size; pool is otherwise sized from deployments + jobs to manage.
   */
  private static final int MAX_PARALLEL_POOL_SIZE = 100;

  private static final TypeReference<List<DeploymentEnvUpdate>> LIST_DEPLOYMENT_ENV_UPDATE =
      new TypeReference<List<DeploymentEnvUpdate>>() {};

  @Nullable private final KubernetesScaleDownConfiguration configuration;
  @Nullable private final Supplier<Optional<KubernetesApiAccessor>> accessorSupplier;

  /**
   * Production constructor; accessor is created via {@link KubernetesApiAccessor#createInCluster}.
   */
  public KubernetesScaleDownStep(@Nullable KubernetesScaleDownConfiguration configuration) {
    this(configuration, null);
  }

  /**
   * Constructor for tests: when {@code accessorSupplier} is non-null, it is used instead of
   * createInCluster so a mock accessor can be injected.
   */
  KubernetesScaleDownStep(
      @Nullable KubernetesScaleDownConfiguration configuration,
      @Nullable Supplier<Optional<KubernetesApiAccessor>> accessorSupplier) {
    this.configuration = configuration;
    this.accessorSupplier = accessorSupplier;
  }

  @Override
  public String id() {
    return STEP_ID;
  }

  @Override
  public boolean skip(UpgradeContext context) {
    KubernetesScaleDownConfiguration config = resolveConfig();
    if (config.getKubernetesServiceHost() == null || config.getKubernetesServiceHost().isEmpty()) {
      log.debug("Not running in Kubernetes (kubernetesServiceHost unset); skipping step.");
      return true;
    }
    if (!config.isEnabled()) {
      log.debug("Kubernetes scale-down is disabled; skipping step.");
      return true;
    }
    if (!config.isUseJavaImplementation()) {
      log.debug("Kubernetes scale-down Java implementation disabled; skipping step.");
      return true;
    }
    if (!context.isScaleDownRequired()) {
      log.debug("Scale-down not required by any blocking upgrade; skipping step.");
      return true;
    }
    return false;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return context -> {
      try {
        KubernetesScaleDownConfiguration config = resolveConfig();
        Optional<KubernetesApiAccessor> accessorOpt =
            accessorSupplier != null
                ? accessorSupplier.get()
                : KubernetesApiAccessor.createInCluster(config);
        if (accessorOpt.isEmpty()) {
          log.error("Could not create Kubernetes client; scale-down step cannot run.");
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }
        KubernetesApiAccessor accessor = accessorOpt.get();
        String namespace = KubernetesApiAccessor.getNamespaceFromEnvironment();
        String configMapName = KubernetesApiAccessor.resolveStateConfigMapName(config, namespace);
        ObjectMapper objectMapper = context.opContext().getObjectMapper();

        Optional<ScaleDownState> existing =
            accessor.getConfigMapState(configMapName, namespace, objectMapper);
        if (existing.isPresent()) {
          ScaleDownState state = existing.get();
          int nextAttempt = state.getAttempt() + 1;
          log.info(
              "Scale-down state found (attempt {}); incrementing to {}. maxRetries={}",
              state.getAttempt(),
              nextAttempt,
              config.getMaxRetries());
          state.setAttempt(nextAttempt);
          if (nextAttempt > config.getMaxRetries()) {
            log.warn(
                "Attempt {} exceeds maxRetries {}; restoring previous state and failing.",
                nextAttempt,
                config.getMaxRetries());
            restore(accessor, state, namespace);
            accessor.deleteConfigMap(configMapName, namespace);
            return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
          }
          accessor.createOrReplaceConfigMap(configMapName, namespace, state, objectMapper);
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
        }

        log.info("No scale-down state; capturing current state from cluster and scaling down.");
        ScaleDownState state = captureAndScaleDown(accessor, config, namespace, objectMapper);
        accessor.createOrReplaceConfigMap(configMapName, namespace, state, objectMapper);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("Kubernetes scale-down step failed", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private KubernetesScaleDownConfiguration resolveConfig() {
    return configuration != null ? configuration : new KubernetesScaleDownConfiguration();
  }

  private ScaleDownState captureAndScaleDown(
      KubernetesApiAccessor accessor,
      KubernetesScaleDownConfiguration config,
      String namespace,
      ObjectMapper objectMapper) {
    List<DeploymentEnvUpdate> envUpdates = getDeploymentEnvUpdates(config, objectMapper);
    Map<String, Map<String, String>> envBeforeByDeployment = new LinkedHashMap<>();
    List<ScaleDownState.DeploymentReplicas> deployments = new ArrayList<>();
    Set<String> deploymentNamesInState = new LinkedHashSet<>();

    for (DeploymentEnvUpdate update : envUpdates) {
      if (update.getLabelSelector() == null || update.getLabelSelector().isBlank()) continue;
      if (update.getEnv() == null || update.getEnv().isEmpty()) continue;
      String name = accessor.getDeploymentNameByLabel(update.getLabelSelector().trim(), namespace);
      if (name == null || name.isEmpty()) continue;
      int replicas = accessor.getDeploymentReplicas(name, namespace);
      deployments.add(
          ScaleDownState.DeploymentReplicas.builder().name(name).replicas(replicas).build());
      deploymentNamesInState.add(name);
      Map<String, String> currentEnv = accessor.getDeploymentEnv(name, namespace);
      Map<String, String> before = new HashMap<>();
      for (String key : update.getEnv().keySet()) {
        before.put(key, currentEnv.getOrDefault(key, ""));
      }
      envBeforeByDeployment.put(name, before);
    }

    List<String> scaleDownLabelSelectors = parseScaleDownLabelSelectors(config);
    List<String> scaleDownNames = resolveScaleDownDeploymentNames(config, accessor, namespace);
    for (String name : scaleDownNames) {
      if (deploymentNamesInState.add(name)) {
        deployments.add(
            ScaleDownState.DeploymentReplicas.builder()
                .name(name)
                .replicas(accessor.getDeploymentReplicas(name, namespace))
                .build());
      }
    }

    runParallel(
        envBeforeByDeployment.keySet().stream()
            .map(name -> (Runnable) () -> accessor.waitForRollout(name, namespace))
            .collect(Collectors.toList()));

    List<String> jobNames = accessor.listActiveJobNamesExceptSystemUpdate(namespace);
    runParallel(
        jobNames.stream()
            .map(jobName -> (Runnable) () -> accessor.deleteJob(jobName, namespace))
            .collect(Collectors.toList()));

    int deploymentsAndJobs =
        scaleDownNames.size() + envBeforeByDeployment.size() + jobNames.size() + 2;
    int poolSize = Math.min(MAX_PARALLEL_POOL_SIZE, Math.max(deploymentsAndJobs, 2));
    ExecutorService executor = Executors.newFixedThreadPool(poolSize);

    try {
      if (!scaleDownNames.isEmpty()) {
        runParallel(
            scaleDownNames.stream()
                .map(name -> (Runnable) () -> accessor.deleteScaledObject(name, namespace))
                .collect(Collectors.toList()),
            executor);
        runParallel(
            scaleDownNames.stream()
                .map(name -> (Runnable) () -> accessor.scaleDeployment(name, namespace, 0))
                .collect(Collectors.toList()),
            executor);
        List<Runnable> envUpdateTasks = new ArrayList<>();
        for (DeploymentEnvUpdate update : envUpdates) {
          if (update.getLabelSelector() == null || update.getEnv() == null) continue;
          String name =
              accessor.getDeploymentNameByLabel(update.getLabelSelector().trim(), namespace);
          if (name != null && !name.isEmpty()) {
            Map<String, String> env = update.getEnv();
            envUpdateTasks.add(
                () -> {
                  accessor.setDeploymentEnv(name, namespace, env);
                  accessor.waitForRollout(name, namespace);
                });
          }
        }
        runParallel(envUpdateTasks, executor);
        runParallel(
            scaleDownNames.stream()
                .map(name -> (Runnable) () -> accessor.waitForRollout(name, namespace))
                .collect(Collectors.toList()),
            executor);
      } else {
        List<Runnable> envUpdateTasks = new ArrayList<>();
        for (DeploymentEnvUpdate update : envUpdates) {
          if (update.getLabelSelector() == null || update.getEnv() == null) continue;
          String name =
              accessor.getDeploymentNameByLabel(update.getLabelSelector().trim(), namespace);
          if (name != null && !name.isEmpty()) {
            Map<String, String> env = update.getEnv();
            envUpdateTasks.add(
                () -> {
                  accessor.setDeploymentEnv(name, namespace, env);
                  accessor.waitForRollout(name, namespace);
                });
          }
        }
        runParallel(envUpdateTasks, executor);
      }
    } finally {
      executor.shutdown();
    }

    return ScaleDownState.builder()
        .attempt(1)
        .scaleDownLabelSelectors(scaleDownLabelSelectors)
        .scaleDownDeploymentNames(scaleDownNames)
        .deployments(deployments)
        .envBeforeByDeployment(envBeforeByDeployment)
        .build();
  }

  /**
   * Parses scaleDownDeploymentLabelSelectors (comma-separated) into a list. Returns empty list if
   * unset or blank; never null.
   */
  private List<String> parseScaleDownLabelSelectors(KubernetesScaleDownConfiguration config) {
    String raw = config.getScaleDownDeploymentLabelSelectors();
    if (raw == null || raw.isBlank()) {
      return Collections.emptyList();
    }
    return Arrays.stream(raw.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }

  /**
   * Resolves deployment names to scale to zero from scaleDownDeploymentLabelSelectors
   * (comma-separated). Missing deployments are skipped; order is preserved, duplicates removed.
   */
  private List<String> resolveScaleDownDeploymentNames(
      KubernetesScaleDownConfiguration config, KubernetesApiAccessor accessor, String namespace) {
    String raw = config.getScaleDownDeploymentLabelSelectors();
    if (raw == null || raw.isBlank()) {
      return Collections.emptyList();
    }
    List<String> selectors =
        Arrays.stream(raw.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList());
    Set<String> seen = new LinkedHashSet<>();
    for (String selector : selectors) {
      String name = accessor.getDeploymentNameByLabel(selector, namespace);
      if (name != null && !name.isEmpty()) {
        seen.add(name);
      }
    }
    return new ArrayList<>(seen);
  }

  /**
   * Runs all tasks in parallel and waits for completion. Propagates the first exception if any task
   * fails. Uses the default fork-join pool.
   */
  private static void runParallel(List<Runnable> tasks) {
    if (tasks.isEmpty()) return;
    CompletableFuture<?>[] futures =
        tasks.stream().map(t -> CompletableFuture.runAsync(t)).toArray(CompletableFuture[]::new);
    CompletableFuture.allOf(futures).join();
  }

  /**
   * Runs all tasks in parallel using the given executor and waits for completion. Propagates the
   * first exception if any task fails.
   */
  private static void runParallel(List<Runnable> tasks, ExecutorService executor) {
    if (tasks.isEmpty()) return;
    CompletableFuture<?>[] futures =
        tasks.stream()
            .map(t -> CompletableFuture.runAsync(t, executor))
            .toArray(CompletableFuture[]::new);
    CompletableFuture.allOf(futures).join();
  }

  private void restore(KubernetesApiAccessor accessor, ScaleDownState state, String namespace) {
    List<ScaleDownState.DeploymentReplicas> deployments = state.getDeployments();
    if (deployments != null && !deployments.isEmpty()) {
      runParallel(
          deployments.stream()
              .map(
                  dr ->
                      (Runnable)
                          () -> accessor.scaleDeployment(dr.getName(), namespace, dr.getReplicas()))
              .collect(Collectors.toList()));
    }
    if (state.getEnvBeforeByDeployment() != null && !state.getEnvBeforeByDeployment().isEmpty()) {
      runParallel(
          state.getEnvBeforeByDeployment().entrySet().stream()
              .filter(e -> e.getValue() != null && !e.getValue().isEmpty())
              .map(
                  e ->
                      (Runnable)
                          () -> accessor.setDeploymentEnv(e.getKey(), namespace, e.getValue()))
              .collect(Collectors.toList()));
    }
    if (deployments != null && !deployments.isEmpty()) {
      runParallel(
          deployments.stream()
              .map(dr -> (Runnable) () -> accessor.waitForRollout(dr.getName(), namespace))
              .collect(Collectors.toList()));
    }
  }

  /**
   * Parses deploymentEnvUpdates JSON (array of { labelSelector, env }). Returns empty list if unset
   * or invalid; never null.
   */
  private List<DeploymentEnvUpdate> getDeploymentEnvUpdates(
      KubernetesScaleDownConfiguration config, ObjectMapper objectMapper) {
    String json = config.getDeploymentEnvUpdates();
    if (json == null || json.isBlank()) {
      return Collections.emptyList();
    }
    try {
      List<DeploymentEnvUpdate> list = objectMapper.readValue(json, LIST_DEPLOYMENT_ENV_UPDATE);
      return list != null ? list : Collections.emptyList();
    } catch (Exception e) {
      throw new IllegalStateException(
          "systemUpdate.kubernetesScaleDown.deploymentEnvUpdates must be a JSON array of { labelSelector, env }",
          e);
    }
  }
}
