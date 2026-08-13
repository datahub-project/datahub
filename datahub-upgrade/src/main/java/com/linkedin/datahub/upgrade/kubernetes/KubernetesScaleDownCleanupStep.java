package com.linkedin.datahub.upgrade.kubernetes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeResult;
import com.linkedin.metadata.config.kubernetes.KubernetesScaleDownConfiguration;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Cleanup step that restores the state captured during scale-down when the upgrade completes
 * successfully, then deletes the state ConfigMap. Ensures the next upgrade run starts with no stale
 * state.
 */
@Slf4j
public class KubernetesScaleDownCleanupStep implements UpgradeCleanupStep {

  private static final String STEP_ID = "KubernetesScaleDownCleanupStep";

  @Nullable private final KubernetesScaleDownConfiguration configuration;
  @Nullable private final Supplier<Optional<KubernetesApiAccessor>> accessorSupplier;

  public KubernetesScaleDownCleanupStep(
      @Nullable KubernetesScaleDownConfiguration configuration) {
    this(configuration, null);
  }

  KubernetesScaleDownCleanupStep(
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
  public BiConsumer<UpgradeContext, UpgradeResult> executable() {
    return (context, result) -> {
      if (result.result() != DataHubUpgradeState.SUCCEEDED) {
        log.debug("Upgrade did not succeed; leaving scale-down state ConfigMap for retry/restore.");
        return;
      }
      KubernetesScaleDownConfiguration config = resolveConfig();
      if (config.getKubernetesServiceHost() == null
          || config.getKubernetesServiceHost().isEmpty()) {
        log.debug("Not in Kubernetes; skipping scale-down state cleanup.");
        return;
      }
      if (!config.isEnabled()) {
        log.debug("Kubernetes scale-down disabled; skipping cleanup.");
        return;
      }
      if (!config.isUseJavaImplementation()) {
        log.debug("Kubernetes scale-down Java implementation disabled; skipping cleanup.");
        return;
      }
      Optional<KubernetesApiAccessor> accessorOpt =
          accessorSupplier != null
              ? accessorSupplier.get()
              : KubernetesApiAccessor.createInCluster(config);
      if (accessorOpt.isEmpty()) {
        log.warn(
            "Could not create Kubernetes client; skipping scale-down state ConfigMap cleanup.");
        return;
      }
      KubernetesApiAccessor accessor = accessorOpt.get();
      String namespace = KubernetesApiAccessor.getNamespaceFromEnvironment();
      String configMapName = KubernetesApiAccessor.resolveStateConfigMapName(config, namespace);
      ObjectMapper objectMapper = context.opContext().getObjectMapper();
      Optional<ScaleDownState> state =
          accessor.getConfigMapState(configMapName, namespace, objectMapper);
      if (state.isEmpty()) {
        log.debug("No scale-down state ConfigMap found; skipping cleanup.");
        return;
      }
      try {
        KubernetesScaleDownStep.restore(accessor, state.get(), namespace);
        accessor.deleteConfigMap(configMapName, namespace);
      } catch (RuntimeException e) {
        log.error(
            "Failed to restore Kubernetes scale-down state; leaving ConfigMap {} for recovery.",
            configMapName,
            e);
        throw e;
      }
    };
  }

  private KubernetesScaleDownConfiguration resolveConfig() {
    return configuration != null ? configuration : new KubernetesScaleDownConfiguration();
  }
}
