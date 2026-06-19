package com.linkedin.metadata.config.kubernetes;

import lombok.Data;

/**
 * Configuration for Kubernetes scale-down during system-update blocking upgrade.
 *
 * <p>Loaded from application.yaml under systemUpdate.kubernetesScaleDown
 *
 * <p>When running in Kubernetes, the upgrade job can scale down GMS/MAE/MCE (and optionally restore
 * after max retries on failure). This config drives enabled state, retry limit, state ConfigMap
 * name, and which deployments/env vars to manage.
 */
@Data
public class KubernetesScaleDownConfiguration {

  /**
   * When non-empty, indicates the process is running inside a Kubernetes cluster. Populated from
   * the global {@code kubernetes.serviceHost} (see {@link KubernetesConfiguration}) when the
   * scale-down config is resolved. Step skips when this is null or empty.
   */
  private String kubernetesServiceHost;

  /**
   * Whether K8 scale-down is enabled (step still skips when not in K8). Respects legacy Helm flag.
   */
  private boolean enabled;

  /**
   * When true, the Java-based scale-down step runs; when false, the step skips even if scale-down
   * is enabled. Default false (set in application.yaml); enables opt-in to the Java implementation.
   */
  private boolean useJavaImplementation;

  /** Max number of scale-down step runs across job restarts before restoring and failing. */
  private int maxRetries;

  /**
   * ConfigMap name for persisting scale-down state (namespace from pod). Empty = derive from
   * release/job (e.g. {release}-system-update-scale-down-state).
   */
  private String stateConfigMapName;

  /**
   * Label selectors for deployments to scale to zero (comma-separated). Each selector resolves to
   * one deployment; deployments matching any selector are scaled to zero and their KEDA
   * ScaledObjects removed. Example:
   * "app.kubernetes.io/name=datahub-mae-consumer,app.kubernetes.io/name=datahub-mce-consumer". Set
   * via Helm or application.yaml. Missing deployments are skipped.
   */
  private String scaleDownDeploymentLabelSelectors;

  /**
   * JSON array of { "labelSelector": "...", "env": { "KEY": "value", ... } }. Each entry identifies
   * a deployment by label selector and the env vars to set when scaling down. State stores previous
   * env per deployment for restore. Example:
   * [{"labelSelector":"app.kubernetes.io/name=datahub-gms","env":{"PRE_PROCESS_HOOKS_UI_ENABLED":"false"}}].
   * Set via Helm (DATAHUB_UPGRADE_K8_DEPLOYMENT_ENV_UPDATES) or application.yaml.
   */
  private String deploymentEnvUpdates;

  /**
   * KEDA ScaledObject CRD API group (e.g. keda.sh). When KEDA is not installed, ScaledObject ops
   * are no-ops.
   */
  private String kedaGroup;

  /** KEDA ScaledObject CRD API version (e.g. v1alpha1). */
  private String kedaVersion;

  /** KEDA ScaledObject resource plural (e.g. scaledobjects). */
  private String kedaScaledObjectsPlural;

  /** Seconds between polls when waiting for deployment rollout. */
  private int rolloutPollSeconds;

  /**
   * Max seconds to wait for each deployment rollout (scale down or rolling restart) before timing
   * out. Default 1800 (30 min); increase for many pods or slow rollout strategies.
   */
  private int rolloutMaxWaitSeconds;

  /**
   * Minimum seconds between INFO logs while polling rollout status (avoids log spam). Defaults to
   * 60 when unset or non-positive. Use a small value in tests to exercise progress logging without
   * long waits.
   */
  private int rolloutProgressLogIntervalSeconds;
}
