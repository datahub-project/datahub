package io.datahubproject.openapi.config;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Factory for Kubernetes-related beans: the in-cluster client and an ObjectMapper configured for
 * serializing K8s API responses (strips managedFields, ownerReferences). The client is only created
 * if the application is running in a Kubernetes environment. Fabric8 handles all environment
 * detection automatically.
 */
@Slf4j
@Configuration
public class KubernetesClientFactory {

  /** Fields to exclude from ObjectMeta in API responses. */
  private static final Set<String> EXCLUDED_OBJECT_META_FIELDS =
      Set.of("managedFields", "ownerReferences");

  private static final String OBJECT_META_FILTER = "objectMetaFilter";

  @JsonFilter(OBJECT_META_FILTER)
  abstract static class ObjectMetaFilterMixin {}

  private static final ObjectMapper KUBERNETES_OBJECT_MAPPER = createKubernetesObjectMapper();

  private static ObjectMapper createKubernetesObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    SimpleBeanPropertyFilter filter =
        SimpleBeanPropertyFilter.serializeAllExcept(EXCLUDED_OBJECT_META_FIELDS);
    SimpleFilterProvider filterProvider = new SimpleFilterProvider();
    filterProvider.addFilter(OBJECT_META_FILTER, filter);
    mapper.addMixIn(ObjectMeta.class, ObjectMetaFilterMixin.class);
    mapper.setFilterProvider(filterProvider);
    return mapper;
  }

  /**
   * Returns the ObjectMapper configured for Kubernetes API responses. This mapper strips verbose
   * metadata fields (managedFields, ownerReferences) that add noise without value to API consumers.
   * Call this when not using Spring DI; otherwise inject the {@code kubernetesObjectMapper} bean.
   */
  public static ObjectMapper getObjectMapper() {
    return KUBERNETES_OBJECT_MAPPER;
  }

  @Bean(name = "kubernetesObjectMapper")
  public ObjectMapper kubernetesObjectMapper() {
    return getObjectMapper();
  }

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
