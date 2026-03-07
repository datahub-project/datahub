package io.datahubproject.openapi.config;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Factory for creating a Kubernetes client that is configured to interact with the cluster the
 * application is deployed in. The client is only created if the application is running in a
 * Kubernetes environment. Fabric8 handles all environment detection automatically.
 */
@Slf4j
@Configuration
public class KubernetesClientFactory {

  @Bean(name = "kubernetesClient")
  @Nullable
  public KubernetesClient createKubernetesClient() {
    try {
      // Fabric8 auto-configures everything when running in K8s:
      // - API server URL from KUBERNETES_SERVICE_HOST/PORT
      // - Auth token from /var/run/secrets/kubernetes.io/serviceaccount/token
      // - Namespace from /var/run/secrets/kubernetes.io/serviceaccount/namespace
      // - CA cert from /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
      // If not running in K8s, this will fail and we return null
      KubernetesClient client = new KubernetesClientBuilder().build();

      // Verify we can actually connect (will fail if not in K8s)
      String namespace = client.getConfiguration().getNamespace();
      String masterUrl = client.getConfiguration().getMasterUrl();

      if (namespace == null || masterUrl == null) {
        log.info("Kubernetes environment not detected - K8s operations API disabled");
        client.close();
        return null;
      }

      log.info("Kubernetes environment detected - K8s operations API enabled");
      log.info("  Namespace: {}", namespace);
      log.info("  API Server: {}", masterUrl);
      log.warn(
          "WARNING: The K8s operations API allows modification of cluster resources. "
              + "Use with caution in production environments. "
              + "Disable RBAC (global.rbac.k8sOperations.create=false) if not needed.");

      return client;
    } catch (Exception e) {
      log.info(
          "Kubernetes environment not detected - K8s operations API disabled ({})", e.getMessage());
      return null;
    }
  }
}
