package io.datahubproject.openapi.config;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import java.util.Set;

/**
 * Jackson configuration for Kubernetes API responses.
 *
 * <p>Provides a dedicated ObjectMapper for the K8s controller that strips verbose metadata fields
 * like managedFields and ownerReferences. This ObjectMapper is scoped to K8s endpoints only and
 * does not affect global JSON serialization.
 *
 * <p>This is NOT a Spring @Configuration - the ObjectMapper is created directly by the controller
 * to avoid polluting the global bean context.
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

    // Create a filter that excludes the verbose fields
    SimpleBeanPropertyFilter filter = SimpleBeanPropertyFilter.serializeAllExcept(EXCLUDED_FIELDS);

    // Register the filter with a provider
    SimpleFilterProvider filterProvider = new SimpleFilterProvider();
    filterProvider.addFilter(OBJECT_META_FILTER, filter);

    // Apply the filter mixin to ObjectMeta
    mapper.addMixIn(ObjectMeta.class, ObjectMetaFilterMixin.class);
    mapper.setFilterProvider(filterProvider);

    return mapper;
  }

  /**
   * Gets the ObjectMapper configured for Kubernetes API responses. This mapper strips verbose
   * metadata fields that add noise without value to API consumers.
   */
  public static ObjectMapper getObjectMapper() {
    return INSTANCE;
  }
}
