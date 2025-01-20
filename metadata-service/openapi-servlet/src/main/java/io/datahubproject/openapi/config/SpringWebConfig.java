package io.datahubproject.openapi.config;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.openapi.v3.OpenAPIV3Generator;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.servers.Server;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springdoc.core.models.GroupedOpenApi;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@EnableWebMvc
@OpenAPIDefinition(
    info = @Info(title = "DataHub OpenAPI", version = "2.0.0"),
    servers = {@Server(url = "/openapi/", description = "Default Server URL")})
@Order(2)
@Configuration
public class SpringWebConfig implements WebMvcConfigurer {
  private static final Set<String> OPERATIONS_PACKAGES =
      Set.of("io.datahubproject.openapi.operations", "io.datahubproject.openapi.health");
  private static final Set<String> V1_PACKAGES = Set.of("io.datahubproject.openapi.v1");
  private static final Set<String> V2_PACKAGES = Set.of("io.datahubproject.openapi.v2");
  private static final Set<String> V3_PACKAGES = Set.of("io.datahubproject.openapi.v3");

  private static final Set<String> OPENLINEAGE_PACKAGES =
      Set.of("io.datahubproject.openapi.openlineage");

  @Bean
  public GroupedOpenApi v3OpenApiGroup(
      final EntityRegistry entityRegistry, final ConfigurationProvider configurationProvider) {
    return GroupedOpenApi.builder()
        .group("10-openapi-v3")
        .displayName("DataHub v3 (OpenAPI)")
        .addOpenApiCustomizer(
            openApi -> {
              OpenAPI v3OpenApi =
                  OpenAPIV3Generator.generateOpenApiSpec(entityRegistry, configurationProvider);
              openApi.setInfo(v3OpenApi.getInfo());
              openApi.setTags(Collections.emptyList());
              openApi.getPaths().putAll(v3OpenApi.getPaths());
              // Merge components. Swagger does not provide append method to add components.
              final Components components = new Components();
              final Components oComponents = openApi.getComponents();
              final Components v3Components = v3OpenApi.getComponents();
              components
                  .callbacks(concat(oComponents::getCallbacks, v3Components::getCallbacks))
                  .examples(concat(oComponents::getExamples, v3Components::getExamples))
                  .extensions(concat(oComponents::getExtensions, v3Components::getExtensions))
                  .headers(concat(oComponents::getHeaders, v3Components::getHeaders))
                  .links(concat(oComponents::getLinks, v3Components::getLinks))
                  .parameters(concat(oComponents::getParameters, v3Components::getParameters))
                  .requestBodies(
                      concat(oComponents::getRequestBodies, v3Components::getRequestBodies))
                  .responses(concat(oComponents::getResponses, v3Components::getResponses))
                  .schemas(concat(oComponents::getSchemas, v3Components::getSchemas))
                  .securitySchemes(
                      concat(oComponents::getSecuritySchemes, v3Components::getSecuritySchemes));
              openApi.setComponents(components);
            })
        .packagesToScan(V3_PACKAGES.toArray(String[]::new))
        .build();
  }

  @Bean
  public GroupedOpenApi openApiGroupV2() {
    return GroupedOpenApi.builder()
        .group("20-openapi-v2")
        .displayName("DataHub v2 (OpenAPI)")
        .packagesToScan(V2_PACKAGES.toArray(String[]::new))
        .build();
  }

  @Bean
  public GroupedOpenApi openApiGroupV1() {
    return GroupedOpenApi.builder()
        .group("30-openapi-v1")
        .displayName("DataHub v1 (OpenAPI)")
        .packagesToScan(V1_PACKAGES.toArray(String[]::new))
        .build();
  }

  @Bean
  public GroupedOpenApi operationsOpenApiGroup() {
    return GroupedOpenApi.builder()
        .group("40-operations")
        .displayName("Operations")
        .packagesToScan(OPERATIONS_PACKAGES.toArray(String[]::new))
        .build();
  }

  @Bean
  public GroupedOpenApi openlineageOpenApiGroup() {
    return GroupedOpenApi.builder()
        .group("50-openlineage")
        .displayName("OpenLineage")
        .packagesToScan(OPENLINEAGE_PACKAGES.toArray(String[]::new))
        .build();
  }

  @Override
  public void addResourceHandlers(ResourceHandlerRegistry registry) {
    registry
        .addResourceHandler("/swagger-ui/**")
        .addResourceLocations("classpath:/META-INF/resources/webjars/swagger-ui/");
  }

  /** Concatenates two maps. */
  private <K, V> Map<K, V> concat(Supplier<Map<K, V>> a, Supplier<Map<K, V>> b) {
    return a.get() == null
        ? b.get()
        : b.get() == null
            ? a.get()
            : Stream.concat(a.get().entrySet().stream(), b.get().entrySet().stream())
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (v1, v2) -> v2,
                        LinkedHashMap::new));
  }
}
