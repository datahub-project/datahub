package com.linkedin.datahub.upgrade.kubernetes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.kubernetes.KubernetesScaleDownConfiguration;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.ListOptionsBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext;
import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Accessor for Kubernetes API using the Fabric8 client (shared foundation with openapi K8s
 * operations). Used by the scale-down step and cleanup step. Creates an in-cluster client when
 * running inside a pod; aligns with {@code
 * io.datahubproject.openapi.config.KubernetesClientFactory}.
 */
@Slf4j
public class KubernetesApiAccessor {

  private final KubernetesClient client;
  @Nullable private final KubernetesScaleDownConfiguration configuration;

  public KubernetesApiAccessor(KubernetesClient client) {
    this(client, null);
  }

  public KubernetesApiAccessor(
      KubernetesClient client, KubernetesScaleDownConfiguration configuration) {
    this.client = client;
    this.configuration = configuration;
  }

  /**
   * Creates an accessor when running inside a Kubernetes cluster (in-cluster config). Returns empty
   * if not in cluster or if client creation fails.
   *
   * @deprecated Use {@link #createInCluster(KubernetesScaleDownConfiguration)} so that KEDA and
   *     rollout operations have required configuration; the no-arg overload leaves configuration
   *     null and those methods will throw.
   */
  @Deprecated
  public static Optional<KubernetesApiAccessor> createInCluster() {
    return createInCluster(null);
  }

  /**
   * Creates an accessor when running inside a Kubernetes cluster, using the given configuration for
   * KEDA CRD and rollout timeout settings.
   */
  public static Optional<KubernetesApiAccessor> createInCluster(
      @Nullable KubernetesScaleDownConfiguration configuration) {
    if (System.getenv("KUBERNETES_SERVICE_HOST") == null
        || System.getenv("KUBERNETES_SERVICE_HOST").isEmpty()) {
      return Optional.empty();
    }
    try {
      KubernetesClient client = new KubernetesClientBuilder().build();
      String namespace = client.getConfiguration().getNamespace();
      String masterUrl = client.getConfiguration().getMasterUrl();
      if (namespace == null || masterUrl == null) {
        client.close();
        return Optional.empty();
      }
      return Optional.of(new KubernetesApiAccessor(client, configuration));
    } catch (Exception e) {
      log.warn("Could not create in-cluster Kubernetes client: {}", e.getMessage());
      return Optional.empty();
    }
  }

  /** KEDA/rollout settings come from systemUpdate.kubernetesScaleDown in application.yaml. */
  private String kedaGroup() {
    if (configuration == null
        || configuration.getKedaGroup() == null
        || configuration.getKedaGroup().isEmpty()) {
      throw new IllegalStateException(
          "KEDA group required: set systemUpdate.kubernetesScaleDown.kedaGroup in application.yaml");
    }
    return configuration.getKedaGroup();
  }

  private String kedaVersion() {
    if (configuration == null
        || configuration.getKedaVersion() == null
        || configuration.getKedaVersion().isEmpty()) {
      throw new IllegalStateException(
          "KEDA version required: set systemUpdate.kubernetesScaleDown.kedaVersion in application.yaml");
    }
    return configuration.getKedaVersion();
  }

  private String kedaScaledObjectsPlural() {
    if (configuration == null
        || configuration.getKedaScaledObjectsPlural() == null
        || configuration.getKedaScaledObjectsPlural().isEmpty()) {
      throw new IllegalStateException(
          "KEDA scaled objects plural required: set systemUpdate.kubernetesScaleDown.kedaScaledObjectsPlural in application.yaml");
    }
    return configuration.getKedaScaledObjectsPlural();
  }

  private int rolloutPollSeconds() {
    if (configuration == null || configuration.getRolloutPollSeconds() <= 0) {
      throw new IllegalStateException(
          "Rollout poll seconds required: set systemUpdate.kubernetesScaleDown.rolloutPollSeconds in application.yaml");
    }
    return configuration.getRolloutPollSeconds();
  }

  private int rolloutMaxWaitSeconds() {
    if (configuration == null || configuration.getRolloutMaxWaitSeconds() <= 0) {
      throw new IllegalStateException(
          "Rollout max wait seconds required: set systemUpdate.kubernetesScaleDown.rolloutMaxWaitSeconds in application.yaml");
    }
    return configuration.getRolloutMaxWaitSeconds();
  }

  /**
   * Reads retry count and last-known restore values from the ConfigMap. Do not use to determine
   * actual cluster state—current state must be read from the Kubernetes API.
   */
  public Optional<ScaleDownState> getConfigMapState(
      String configMapName, String namespace, ObjectMapper objectMapper) {
    try {
      ConfigMap cm = client.configMaps().inNamespace(namespace).withName(configMapName).get();
      if (cm == null || cm.getData() == null || !cm.getData().containsKey("state")) {
        return Optional.empty();
      }
      String json = cm.getData().get("state");
      ScaleDownState state = objectMapper.readValue(json, ScaleDownState.class);
      return Optional.of(state);
    } catch (Exception e) {
      if (isNotFound(e)) {
        return Optional.empty();
      }
      log.debug("Could not get ConfigMap {}: {}", configMapName, e.getMessage());
      return Optional.empty();
    }
  }

  public void createOrReplaceConfigMap(
      String configMapName, String namespace, ScaleDownState state, ObjectMapper objectMapper) {
    String json;
    try {
      json = objectMapper.writeValueAsString(state);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize scale-down state", e);
    }
    ConfigMap cm =
        new ConfigMapBuilder()
            .withNewMetadata()
            .withName(configMapName)
            .withNamespace(namespace)
            .endMetadata()
            .withData(Map.of("state", json))
            .build();
    client.configMaps().inNamespace(namespace).createOrReplace(cm);
  }

  public void deleteConfigMap(String configMapName, String namespace) {
    try {
      client.configMaps().inNamespace(namespace).withName(configMapName).delete();
      log.info("Deleted scale-down state ConfigMap {}", configMapName);
    } catch (Exception e) {
      if (!isNotFound(e)) {
        log.warn("Could not delete ConfigMap {}: {}", configMapName, e.getMessage());
      }
    }
  }

  @Nullable
  public String getDeploymentNameByLabel(String labelSelector, String namespace) {
    try {
      List<Deployment> list =
          client
              .apps()
              .deployments()
              .inNamespace(namespace)
              .list(new ListOptionsBuilder().withLabelSelector(labelSelector).build())
              .getItems();
      if (list == null || list.isEmpty()) {
        return null;
      }
      return list.get(0).getMetadata() != null ? list.get(0).getMetadata().getName() : null;
    } catch (Exception e) {
      log.debug("Could not list deployments by label {}: {}", labelSelector, e.getMessage());
      return null;
    }
  }

  @Nullable
  public String getDeploymentByNamePattern(String pattern, String namespace) {
    if (pattern == null || pattern.isEmpty()) {
      return null;
    }
    try {
      List<Deployment> list = client.apps().deployments().inNamespace(namespace).list().getItems();
      if (list == null) {
        return null;
      }
      String lower = pattern.toLowerCase();
      for (Deployment d : list) {
        if (d.getMetadata() != null
            && d.getMetadata().getName() != null
            && d.getMetadata().getName().toLowerCase().contains(lower)) {
          return d.getMetadata().getName();
        }
      }
    } catch (Exception e) {
      log.debug("Could not list deployments: {}", e.getMessage());
    }
    return null;
  }

  public int getDeploymentReplicas(String deploymentName, String namespace) {
    try {
      Deployment d =
          client.apps().deployments().inNamespace(namespace).withName(deploymentName).get();
      if (d != null && d.getSpec() != null && d.getSpec().getReplicas() != null) {
        return d.getSpec().getReplicas();
      }
    } catch (Exception e) {
      log.warn("Could not get replicas for {}: {}", deploymentName, e.getMessage());
    }
    return 0;
  }

  public Map<String, String> getDeploymentEnv(String deploymentName, String namespace) {
    Map<String, String> result = new HashMap<>();
    try {
      Deployment d =
          client.apps().deployments().inNamespace(namespace).withName(deploymentName).get();
      if (d == null
          || d.getSpec() == null
          || d.getSpec().getTemplate() == null
          || d.getSpec().getTemplate().getSpec() == null
          || d.getSpec().getTemplate().getSpec().getContainers() == null
          || d.getSpec().getTemplate().getSpec().getContainers().isEmpty()) {
        return result;
      }
      List<EnvVar> env = d.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
      if (env != null) {
        for (EnvVar v : env) {
          if (v.getName() != null) {
            result.put(v.getName(), v.getValue() != null ? v.getValue() : "");
          }
        }
      }
    } catch (Exception e) {
      log.warn("Could not get env for {}: {}", deploymentName, e.getMessage());
    }
    return result;
  }

  public void scaleDeployment(String deploymentName, String namespace, int replicas) {
    client.apps().deployments().inNamespace(namespace).withName(deploymentName).scale(replicas);
  }

  public void setDeploymentEnv(
      String deploymentName, String namespace, Map<String, String> envVars) {
    client
        .apps()
        .deployments()
        .inNamespace(namespace)
        .withName(deploymentName)
        .edit(
            dep -> {
              if (dep.getSpec() == null
                  || dep.getSpec().getTemplate() == null
                  || dep.getSpec().getTemplate().getSpec() == null
                  || dep.getSpec().getTemplate().getSpec().getContainers() == null
                  || dep.getSpec().getTemplate().getSpec().getContainers().isEmpty()) {
                return dep;
              }
              List<EnvVar> env =
                  dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
              if (env == null) {
                env = new ArrayList<>();
              }
              Map<String, EnvVar> byName = new HashMap<>();
              for (EnvVar e : env) {
                if (e.getName() != null) {
                  byName.put(e.getName(), e);
                }
              }
              for (Map.Entry<String, String> e : envVars.entrySet()) {
                byName.put(e.getKey(), new EnvVar(e.getKey(), e.getValue(), null));
              }
              dep.getSpec()
                  .getTemplate()
                  .getSpec()
                  .getContainers()
                  .get(0)
                  .setEnv(new ArrayList<>(byName.values()));
              return dep;
            });
  }

  public void waitForRollout(String deploymentName, String namespace) {
    long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(rolloutMaxWaitSeconds());
    while (System.currentTimeMillis() < deadline) {
      Deployment d =
          client.apps().deployments().inNamespace(namespace).withName(deploymentName).get();
      if (d == null || d.getSpec() == null) {
        return;
      }
      Integer desired = d.getSpec().getReplicas();
      Integer updated =
          d.getStatus() != null && d.getStatus().getUpdatedReplicas() != null
              ? d.getStatus().getUpdatedReplicas()
              : 0;
      if (desired != null && desired.equals(updated)) {
        return;
      }
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(rolloutPollSeconds()));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for rollout", e);
      }
    }
    throw new RuntimeException("Timeout waiting for deployment " + deploymentName + " rollout");
  }

  /**
   * Deletes a KEDA ScaledObject by name. No-op when KEDA is not installed (CRD or API not
   * available); logs at debug and returns without failing.
   */
  public void deleteScaledObject(String name, String namespace) {
    try {
      ResourceDefinitionContext context =
          new ResourceDefinitionContext.Builder()
              .withGroup(kedaGroup())
              .withVersion(kedaVersion())
              .withPlural(kedaScaledObjectsPlural())
              .build();
      client.genericKubernetesResources(context).inNamespace(namespace).withName(name).delete();
    } catch (Exception e) {
      if (isKedaUnavailableOrNotFound(e)) {
        log.debug(
            "KEDA ScaledObject {} not present or KEDA not installed: {}", name, e.getMessage());
      } else {
        log.debug("Could not delete ScaledObject {}: {}", name, e.getMessage());
      }
    }
  }

  /** True if the exception indicates the resource is missing or KEDA/CRD is not installed. */
  private static boolean isKedaUnavailableOrNotFound(Exception e) {
    if (e == null) {
      return false;
    }
    if (isNotFound(e)) {
      return true;
    }
    String msg = e.getMessage();
    if (msg == null) {
      return false;
    }
    String lower = msg.toLowerCase();
    return lower.contains("could not find the requested resource")
        || lower.contains("no matches for kind")
        || lower.contains("the server could not find");
  }

  public List<String> listActiveJobNamesExceptSystemUpdate(String namespace) {
    List<String> out = new ArrayList<>();
    try {
      List<io.fabric8.kubernetes.api.model.batch.v1.Job> items =
          client.batch().v1().jobs().inNamespace(namespace).list().getItems();
      if (items == null) {
        return out;
      }
      for (io.fabric8.kubernetes.api.model.batch.v1.Job job : items) {
        String name = job.getMetadata() != null ? job.getMetadata().getName() : null;
        if (name == null || name.contains("system-update")) {
          continue;
        }
        if (job.getStatus() != null
            && job.getStatus().getActive() != null
            && job.getStatus().getActive() > 0) {
          out.add(name);
        }
      }
    } catch (Exception e) {
      log.warn("Could not list jobs: {}", e.getMessage());
    }
    return out;
  }

  public void deleteJob(String jobName, String namespace) {
    try {
      client.batch().v1().jobs().inNamespace(namespace).withName(jobName).delete();
    } catch (Exception e) {
      log.warn("Could not delete job {}: {}", jobName, e.getMessage());
    }
  }

  /**
   * Resolves the ConfigMap name used for scale-down retry/restore state. Not used to determine
   * actual cluster state—only for persisting attempt count and last-known restore values.
   */
  public static String resolveStateConfigMapName(
      KubernetesScaleDownConfiguration config, String namespace) {
    if (config.getStateConfigMapName() != null && !config.getStateConfigMapName().isEmpty()) {
      return config.getStateConfigMapName();
    }
    String release = System.getenv("HELM_RELEASE_NAME");
    if (release != null && !release.isEmpty()) {
      return release + "-system-update-scale-down-state";
    }
    return "datahub-system-update-scale-down-state";
  }

  /** Returns the namespace from env or service account file; callers use this when in cluster. */
  public static String getNamespaceFromEnvironment() {
    String ns = System.getenv("NAMESPACE");
    if (ns != null && !ns.isEmpty()) {
      return ns;
    }
    ns = System.getenv("POD_NAMESPACE");
    if (ns != null && !ns.isEmpty()) {
      return ns;
    }
    File saNs = new File("/var/run/secrets/kubernetes.io/serviceaccount/namespace");
    if (saNs.canRead()) {
      try {
        return Files.readString(saNs.toPath()).trim();
      } catch (Exception e) {
        log.warn("Could not read service account namespace file", e);
      }
    }
    return "default";
  }

  private static boolean isNotFound(Exception e) {
    String msg = e.getMessage();
    return msg != null && (msg.contains("404") || msg.contains("Not Found"));
  }
}
