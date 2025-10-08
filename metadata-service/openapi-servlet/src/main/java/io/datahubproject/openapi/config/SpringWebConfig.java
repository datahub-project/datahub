package io.datahubproject.openapi.config;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.BasePathUtils;
import io.datahubproject.openapi.v3.OpenAPIV3Customizer;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.models.SpecVersion;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.springdoc.core.models.GroupedOpenApi;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@EnableWebMvc
@OpenAPIDefinition(info = @Info(title = "DataHub OpenAPI", version = "2.0.0"))
@Order(2)
@Configuration
@Slf4j
public class SpringWebConfig implements WebMvcConfigurer {
  private static final String LEGACY_VERSION = "3.0.1";
  private static final Set<String> OPERATIONS_PACKAGES =
      Set.of("io.datahubproject.openapi.operations", "io.datahubproject.openapi.health");
  private static final Set<String> V1_PACKAGES = Set.of("io.datahubproject.openapi.v1");
  private static final Set<String> V2_PACKAGES = Set.of("io.datahubproject.openapi.v2");
  private static final Set<String> V3_PACKAGES = Set.of("io.datahubproject.openapi.v3");

  private static final Set<String> OPENLINEAGE_PACKAGES =
      Set.of("io.datahubproject.openapi.openlineage");

  private static final Set<String> EVENTS_PACKAGES = Set.of("io.datahubproject.openapi.v1.event");

  @Autowired private TracingInterceptor tracingInterceptor;

  @Value("${datahub.basePath:}")
  private String datahubBasePath;

  @Bean
  public GroupedOpenApi v3OpenApiGroup(
      final EntityRegistry entityRegistry, final ConfigurationProvider configurationProvider) {
    return GroupedOpenApi.builder()
        .group("openapi-v3")
        .displayName("1. DataHub v3 (OpenAPI)")
        .addOpenApiCustomizer(
            openApi ->
                OpenAPIV3Customizer.customizer(openApi, entityRegistry, configurationProvider))
        .addOpenApiCustomizer(this::configureServerUrl)
        .packagesToScan(V3_PACKAGES.toArray(String[]::new))
        .build();
  }

  @Bean
  public GroupedOpenApi openApiGroupV2() {
    return GroupedOpenApi.builder()
        .group("openapi-v2")
        .displayName("5. DataHub v2 (OpenAPI)")
        .addOpenApiCustomizer(
            openApi -> openApi.specVersion(SpecVersion.V30).openapi(LEGACY_VERSION))
        .addOpenApiCustomizer(this::configureServerUrl)
        .packagesToScan(V2_PACKAGES.toArray(String[]::new))
        .build();
  }

  @Bean
  public GroupedOpenApi openApiGroupV1() {
    return GroupedOpenApi.builder()
        .group("openapi-v1")
        .displayName("6. DataHub v1 (OpenAPI)")
        .addOpenApiCustomizer(
            openApi -> openApi.specVersion(SpecVersion.V30).openapi(LEGACY_VERSION))
        .addOpenApiCustomizer(this::configureServerUrl)
        .packagesToScan(V1_PACKAGES.toArray(String[]::new))
        .build();
  }

  @Bean
  public GroupedOpenApi operationsOpenApiGroup() {
    return GroupedOpenApi.builder()
        .group("operations")
        .displayName("4. Operations")
        .addOpenApiCustomizer(
            openApi -> openApi.specVersion(SpecVersion.V30).openapi(LEGACY_VERSION))
        .addOpenApiCustomizer(this::configureServerUrl)
        .packagesToScan(OPERATIONS_PACKAGES.toArray(String[]::new))
        .build();
  }

  @Bean
  public GroupedOpenApi openlineageOpenApiGroup() {
    return GroupedOpenApi.builder()
        .group("openlineage")
        .displayName("3. OpenLineage")
        .addOpenApiCustomizer(
            openApi -> openApi.specVersion(SpecVersion.V30).openapi(LEGACY_VERSION))
        .addOpenApiCustomizer(this::configureServerUrl)
        .packagesToScan(OPENLINEAGE_PACKAGES.toArray(String[]::new))
        .build();
  }

  @Bean
  @ConditionalOnProperty(name = "eventsApi.enabled", havingValue = "true")
  public GroupedOpenApi eventsOpenApiGroup() {
    return GroupedOpenApi.builder()
        .group("events")
        .displayName("2. Events")
        .addOpenApiCustomizer(
            openApi -> openApi.specVersion(SpecVersion.V30).openapi(LEGACY_VERSION))
        .addOpenApiCustomizer(this::configureServerUrl)
        .packagesToScan(EVENTS_PACKAGES.toArray(String[]::new))
        .build();
  }

  @Override
  public void addResourceHandlers(ResourceHandlerRegistry registry) {
    registry
        .addResourceHandler("/swagger-ui/**")
        .addResourceLocations("classpath:/META-INF/resources/webjars/swagger-ui/");
  }

  private void configureServerUrl(io.swagger.v3.oas.models.OpenAPI openApi) {
    // Clear any existing servers and set a relative server URL with base path
    openApi.setServers(null);
    // Use datahub.basePath for OpenAPI server URLs since they're accessed through frontend proxy
    String serverUrl = BasePathUtils.addBasePath(datahubBasePath, "/");
    openApi.addServersItem(new io.swagger.v3.oas.models.servers.Server().url(serverUrl));
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
