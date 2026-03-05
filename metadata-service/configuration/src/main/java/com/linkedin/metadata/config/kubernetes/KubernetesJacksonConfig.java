package com.linkedin.metadata.config.kubernetes;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import java.util.Set;

/**
 * Jackson configuration for Kubernetes API responses.
 *
 * <p>Provides a dedicated ObjectMapper for serializing K8s resources (e.g. in the OpenAPI K8s
 * controller and system-update scale-down) that strips verbose metadata fields like managedFields
 * and ownerReferences. Shared by metadata-service and datahub-upgrade so both use the same
 * serialization behavior.
 *
 * <p>This is NOT a Spring @Configuration—the ObjectMapper is obtained via {@link
 * #getObjectMapper()} so callers can inject it or use the singleton as needed.
 */
public class KubernetesJacksonConfig {

  /** Fields to exclude from ObjectMeta in API responses. */
  private static final Set<String> EXCLUDED_FIELDS = Set.of("managedFields", "ownerReferences");

  private static final String OBJECT_META_FILTER = "objectMetaFilter";

  /** Mixin to apply the filter to ObjectMeta. */
  @JsonFilter(OBJECT_META_FILTER)
  abstract static class ObjectMetaFilterMixin {}

  /** Singleton instance for reuse. */
  private static final ObjectMapper INSTANCE = createObjectMapper();

  private static ObjectMapper createObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();

    SimpleBeanPropertyFilter filter = SimpleBeanPropertyFilter.serializeAllExcept(EXCLUDED_FIELDS);
    SimpleFilterProvider filterProvider = new SimpleFilterProvider();
    filterProvider.addFilter(OBJECT_META_FILTER, filter);

    mapper.addMixIn(ObjectMeta.class, ObjectMetaFilterMixin.class);
    mapper.setFilterProvider(filterProvider);

    return mapper;
  }

  /**
   * Returns the ObjectMapper configured for Kubernetes API responses. This mapper strips verbose
   * metadata fields that add noise without value to API consumers.
   */
  public static ObjectMapper getObjectMapper() {
    return INSTANCE;
  }

  /**
   * Instance method for dependency injection. Returns the same shared mapper as the static {@link
   * #getObjectMapper()}. Override in tests to supply a custom mapper.
   */
  public ObjectMapper getObjectMapperInstance() {
    return INSTANCE;
  }
}
