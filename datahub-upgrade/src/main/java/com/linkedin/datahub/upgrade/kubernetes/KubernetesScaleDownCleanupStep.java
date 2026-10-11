package com.linkedin.datahub.upgrade.kubernetes;

import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeResult;
import com.linkedin.metadata.config.kubernetes.KubernetesScaleDownConfiguration;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.util.Optional;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Cleanup step that deletes the scale-down state ConfigMap when the upgrade completes successfully.
 * Ensures the next upgrade run starts with no stale state.
 */
@Slf4j
@RequiredArgsConstructor
public class KubernetesScaleDownCleanupStep implements UpgradeCleanupStep {

  private static final String STEP_ID = "KubernetesScaleDownCleanupStep";

  @Nullable private final KubernetesScaleDownConfiguration configuration;

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
      Optional<KubernetesApiAccessor> accessor = KubernetesApiAccessor.createInCluster(config);
      if (accessor.isEmpty()) {
        log.warn(
            "Could not create Kubernetes client; skipping scale-down state ConfigMap cleanup.");
        return;
      }
      String namespace = KubernetesApiAccessor.getNamespaceFromEnvironment();
      String configMapName = KubernetesApiAccessor.resolveStateConfigMapName(config, namespace);
      accessor.get().deleteConfigMap(configMapName, namespace);
    };
  }

  private KubernetesScaleDownConfiguration resolveConfig() {
    return configuration != null ? configuration : new KubernetesScaleDownConfiguration();
  }
}
