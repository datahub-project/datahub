package com.linkedin.metadata.system_info.collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.system_info.SystemInfoDtos.*;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KubernetesCollector {

  public static final String K8_COMPONENT_LABEL_VALUE = "datahub";

  private final KubernetesClient kubernetesClient;

  public KubernetesInfo collect(ExecutorService executorService) {
    if (kubernetesClient == null) {
      return KubernetesInfo.builder().runningInKubernetes(false).build();
    }

    try {
      // Quick connectivity check first
      String namespace = performConnectivityCheck();
      if (namespace == null) {
        return KubernetesInfo.builder().runningInKubernetes(false).build();
      }

      ListOptions listOptions = new ListOptionsBuilder().withTimeoutSeconds(5L).build();

      // Execute all K8s queries in parallel
      CompletableFuture<List<DeploymentInfo>> deploymentsFuture =
          CompletableFuture.supplyAsync(
              () -> getDeployments(namespace, listOptions), executorService);

      CompletableFuture<List<ServiceInfo>> servicesFuture =
          CompletableFuture.supplyAsync(() -> getServices(namespace, listOptions), executorService);

      CompletableFuture<List<ConfigMapInfo>> configMapsFuture =
          CompletableFuture.supplyAsync(
              () -> getConfigMaps(namespace, listOptions), executorService);

      CompletableFuture<String> clusterVersionFuture =
          CompletableFuture.supplyAsync(this::getClusterVersion, executorService);

      CompletableFuture<HelmInfo> helmInfoFuture =
          CompletableFuture.supplyAsync(() -> getHelmInfo(namespace), executorService);

      // Wait for all futures
      CompletableFuture<Void> allFutures =
          CompletableFuture.allOf(
              deploymentsFuture,
              servicesFuture,
              configMapsFuture,
              clusterVersionFuture,
              helmInfoFuture);
      allFutures.get(10, TimeUnit.SECONDS);

      return KubernetesInfo.builder()
          .runningInKubernetes(true)
          .namespace(namespace)
          .deployments(deploymentsFuture.getNow(new ArrayList<>()))
          .services(servicesFuture.getNow(new ArrayList<>()))
          .configMaps(configMapsFuture.getNow(new ArrayList<>()))
          .clusterVersion(clusterVersionFuture.getNow("unknown"))
          .helmInfo(helmInfoFuture.getNow(HelmInfo.builder().installed(false).build()))
          .build();

    } catch (Exception e) {
      log.error("Error fetching Kubernetes information", e);

      if (isNotInKubernetes(e)) {
        return KubernetesInfo.builder().runningInKubernetes(false).build();
      }

      return KubernetesInfo.builder()
          .runningInKubernetes(true)
          .namespace("error: " + e.getMessage())
          .build();
    }
  }

  private String performConnectivityCheck() {
    try {
      String namespace = kubernetesClient.getNamespace();
      if (namespace == null) {
        namespace = "default";
      }

      // Quick connectivity check
      kubernetesClient
          .namespaces()
          .list(new ListOptionsBuilder().withTimeoutSeconds(2L).withLimit(1L).build());

      return namespace;
    } catch (Exception e) {
      log.debug("Not running in Kubernetes environment: {}", e.getMessage());
      return null;
    }
  }

  private boolean isNotInKubernetes(Exception e) {
    return e.getCause() instanceof UnknownHostException
        || (e.getMessage() != null && e.getMessage().contains("kubernetes.default.svc"));
  }

  private List<DeploymentInfo> getDeployments(String namespace, ListOptions listOptions) {
    try {
      return kubernetesClient
          .apps()
          .deployments()
          .inNamespace(namespace)
          .withLabel("app.kubernetes.io/part-of", K8_COMPONENT_LABEL_VALUE)
          .list(listOptions)
          .getItems()
          .stream()
          .map(this::mapDeployment)
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.warn("Failed to list deployments: {}", e.getMessage());
      return new ArrayList<>();
    }
  }

  private List<ServiceInfo> getServices(String namespace, ListOptions listOptions) {
    try {
      return kubernetesClient
          .services()
          .inNamespace(namespace)
          .withLabel("app.kubernetes.io/part-of", K8_COMPONENT_LABEL_VALUE)
          .list(listOptions)
          .getItems()
          .stream()
          .map(this::mapService)
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.warn("Failed to list services: {}", e.getMessage());
      return new ArrayList<>();
    }
  }

  private List<ConfigMapInfo> getConfigMaps(String namespace, ListOptions listOptions) {
    try {
      return kubernetesClient
          .configMaps()
          .inNamespace(namespace)
          .withLabel("app.kubernetes.io/part-of", K8_COMPONENT_LABEL_VALUE)
          .list(listOptions)
          .getItems()
          .stream()
          .map(this::mapConfigMap)
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.warn("Failed to list config maps: {}", e.getMessage());
      return new ArrayList<>();
    }
  }

  private String getClusterVersion() {
    try {
      return kubernetesClient.getKubernetesVersion().getGitVersion();
    } catch (Exception e) {
      log.warn("Failed to get cluster version: {}", e.getMessage());
      return "unknown";
    }
  }

  private HelmInfo getHelmInfo(String namespace) {
    try {
      ListOptions listOptions = new ListOptionsBuilder().withTimeoutSeconds(5L).build();

      List<Secret> helmSecrets =
          kubernetesClient
              .secrets()
              .inNamespace(namespace)
              .withLabel("owner", "helm")
              .list(listOptions)
              .getItems();

      Optional<Secret> dataHubRelease =
          helmSecrets.stream()
              .filter(
                  s -> {
                    String name = s.getMetadata().getLabels().get("name");
                    return name != null && name.contains("datahub");
                  })
              .filter(s -> "deployed".equals(s.getMetadata().getLabels().get("status")))
              .max(
                  Comparator.comparing(
                      s ->
                          Integer.parseInt(
                              s.getMetadata().getLabels().getOrDefault("version", "0"))));

      if (dataHubRelease.isPresent()) {
        return extractHelmInfo(dataHubRelease.get());
      }

      return HelmInfo.builder()
          .installed(false)
          .errorMessage("No Helm release found for DataHub")
          .build();

    } catch (Exception e) {
      log.error("Error getting Helm info", e);

      if (isNotInKubernetes(e)) {
        return HelmInfo.builder()
            .installed(false)
            .errorMessage("Not in Kubernetes environment")
            .build();
      }

      return HelmInfo.builder()
          .installed(false)
          .errorMessage("Error retrieving Helm info: " + e.getMessage())
          .build();
    }
  }

  private DeploymentInfo mapDeployment(Deployment deployment) {
    return DeploymentInfo.builder()
        .name(deployment.getMetadata().getName())
        .replicas(deployment.getSpec().getReplicas())
        .availableReplicas(
            deployment.getStatus().getAvailableReplicas() != null
                ? deployment.getStatus().getAvailableReplicas()
                : 0)
        .labels(deployment.getMetadata().getLabels())
        .image(deployment.getSpec().getTemplate().getSpec().getContainers().get(0).getImage())
        .createdAt(LocalDateTime.parse(deployment.getMetadata().getCreationTimestamp()))
        .build();
  }

  private ServiceInfo mapService(Service service) {
    return ServiceInfo.builder()
        .name(service.getMetadata().getName())
        .type(service.getSpec().getType())
        .clusterIP(service.getSpec().getClusterIP())
        .ports(
            service.getSpec().getPorts().stream()
                .map(ServicePort::getPort)
                .collect(Collectors.toList()))
        .selector(service.getSpec().getSelector())
        .build();
  }

  private ConfigMapInfo mapConfigMap(ConfigMap configMap) {
    return ConfigMapInfo.builder()
        .name(configMap.getMetadata().getName())
        .data(configMap.getData())
        .build();
  }

  private HelmInfo extractHelmInfo(Secret helmSecret) {
    try {
      Map<String, String> labels = helmSecret.getMetadata().getLabels();
      String releaseName = labels.get("name");
      String releaseData = helmSecret.getData().get("release");

      byte[] decodedData = Base64.getDecoder().decode(releaseData);
      byte[] decompressedData = decompress(decodedData);

      String status = labels.get("status");
      String version = labels.get("version");
      String modifiedAt = labels.get("modifiedAt");

      LocalDateTime lastDeployed = null;
      if (modifiedAt != null) {
        try {
          lastDeployed = LocalDateTime.parse(modifiedAt.replace("Z", ""));
        } catch (Exception e) {
          log.debug("Could not parse modified time: {}", modifiedAt);
        }
      }

      Map<String, Object> values = extractValuesFromRelease(decompressedData);

      return HelmInfo.builder()
          .installed(true)
          .releaseName(releaseName)
          .status(status)
          .namespace(helmSecret.getMetadata().getNamespace())
          .values(values)
          .lastDeployed(lastDeployed)
          .chartVersion(version)
          .build();

    } catch (Exception e) {
      log.error("Error parsing Helm release", e);
      return HelmInfo.builder()
          .installed(true)
          .releaseName(helmSecret.getMetadata().getLabels().get("name"))
          .errorMessage("Error parsing release data: " + e.getMessage())
          .build();
    }
  }

  private byte[] decompress(byte[] compressed) throws IOException {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
        GZIPInputStream gis = new GZIPInputStream(bis);
        ByteArrayOutputStream bos = new ByteArrayOutputStream()) {

      byte[] buffer = new byte[1024];
      int len;
      while ((len = gis.read(buffer)) != -1) {
        bos.write(buffer, 0, len);
      }
      return bos.toByteArray();
    }
  }

  private Map<String, Object> extractValuesFromRelease(byte[] releaseData) {
    try {
      String dataStr = new String(releaseData, StandardCharsets.UTF_8);
      String[] markers = {"\"config\":", "\"values\":", "\"userValues\":"};

      for (String marker : markers) {
        int markerIndex = dataStr.indexOf(marker);
        if (markerIndex != -1) {
          int jsonStart = dataStr.indexOf("{", markerIndex);
          if (jsonStart != -1) {
            int depth = 0;
            for (int i = jsonStart; i < dataStr.length(); i++) {
              if (dataStr.charAt(i) == '{') depth++;
              if (dataStr.charAt(i) == '}') depth--;
              if (depth == 0) {
                try {
                  String valuesJson = dataStr.substring(jsonStart, i + 1);
                  ObjectMapper mapper = new ObjectMapper();
                  return mapper.readValue(valuesJson, new TypeReference<Map<String, Object>>() {});
                } catch (Exception e) {
                  log.debug("Failed to parse JSON from marker {}: {}", marker, e.getMessage());
                }
                break;
              }
            }
          }
        }
      }

      log.debug("Could not extract values from Helm release data");
      return new HashMap<>();

    } catch (Exception e) {
      log.error("Error extracting values from release", e);
      return new HashMap<>();
    }
  }
}
