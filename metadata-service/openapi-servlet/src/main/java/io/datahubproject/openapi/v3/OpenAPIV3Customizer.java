package io.datahubproject.openapi.v3;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

public class OpenAPIV3Customizer {
  private OpenAPIV3Customizer() {}

  public static void customizer(
      OpenAPI springOpenAPI,
      EntityRegistry entityRegistry,
      final ConfigurationProvider configurationProvider) {
    OpenAPI registryOpenAPI =
        OpenAPIV3Generator.generateOpenApiSpec(entityRegistry, configurationProvider);

    springOpenAPI.specVersion(registryOpenAPI.getSpecVersion());
    springOpenAPI.openapi(registryOpenAPI.getOpenapi());
    springOpenAPI.setInfo(registryOpenAPI.getInfo());
    springOpenAPI.setTags(Collections.emptyList());

    // Merge paths with null checking
    if (registryOpenAPI.getPaths() != null) {
      if (springOpenAPI.getPaths() == null) {
        springOpenAPI.setPaths(new Paths());
      }
      springOpenAPI.getPaths().putAll(registryOpenAPI.getPaths());
    }

    // Merge components with custom ones taking precedence
    final Components components = new Components();
    final Components oComponents = springOpenAPI.getComponents();
    final Components v3Components = registryOpenAPI.getComponents();

    components
        .callbacks(
            mergeWithPrecedence(
                oComponents != null ? oComponents::getCallbacks : () -> null,
                v3Components != null ? v3Components::getCallbacks : () -> null))
        .examples(
            mergeWithPrecedence(
                oComponents != null ? oComponents::getExamples : () -> null,
                v3Components != null ? v3Components::getExamples : () -> null))
        .extensions(
            mergeWithPrecedence(
                oComponents != null ? oComponents::getExtensions : () -> null,
                v3Components != null ? v3Components::getExtensions : () -> null))
        .headers(
            mergeWithPrecedence(
                oComponents != null ? oComponents::getHeaders : () -> null,
                v3Components != null ? v3Components::getHeaders : () -> null))
        .links(
            mergeWithPrecedence(
                oComponents != null ? oComponents::getLinks : () -> null,
                v3Components != null ? v3Components::getLinks : () -> null))
        .parameters(
            mergeWithPrecedence(
                oComponents != null ? oComponents::getParameters : () -> null,
                v3Components != null ? v3Components::getParameters : () -> null))
        .requestBodies(
            mergeWithPrecedence(
                oComponents != null ? oComponents::getRequestBodies : () -> null,
                v3Components != null ? v3Components::getRequestBodies : () -> null))
        .responses(
            mergeWithPrecedence(
                oComponents != null ? oComponents::getResponses : () -> null,
                v3Components != null ? v3Components::getResponses : () -> null))
        .schemas(
            mergeWithPrecedence(
                oComponents != null ? oComponents::getSchemas : () -> null,
                v3Components != null ? v3Components::getSchemas : () -> null))
        .securitySchemes(
            mergeWithPrecedence(
                oComponents != null ? oComponents::getSecuritySchemes : () -> null,
                v3Components != null ? v3Components::getSecuritySchemes : () -> null));
    springOpenAPI.setComponents(components);
  }

  private static <T> Map<String, T> mergeWithPrecedence(
      Supplier<Map<String, T>> springComponents, Supplier<Map<String, T>> registryComponents) {
    Map<String, T> result = new LinkedHashMap<>();

    // First add Spring-generated components
    Map<String, T> springMap = springComponents.get();
    if (springMap != null) {
      result.putAll(springMap);
    }

    // Then add custom components, which will override any conflicts
    Map<String, T> customMap = registryComponents.get();
    if (customMap != null) {
      result.putAll(customMap);
    }

    return result;
  }
}
