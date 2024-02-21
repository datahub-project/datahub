package io.datahubproject.openapi.config;

import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.openapi.converter.StringToChangeCategoryConverter;
import io.datahubproject.openapi.v3.OpenAPIV3Generator;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.servers.Server;
import io.swagger.v3.oas.models.OpenAPI;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.springdoc.core.models.GroupedOpenApi;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.format.FormatterRegistry;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@EnableWebMvc
@OpenAPIDefinition(
    info = @Info(title = "DataHub OpenAPI", version = "2.0.0"),
    servers = {@Server(url = "/openapi/", description = "Default Server URL")})
@Configuration
public class SpringWebConfig implements WebMvcConfigurer {
  private static final Set<String> OPERATIONS_PACKAGES =
      Set.of("io.datahubproject.openapi.operations", "io.datahubproject.openapi.health");
  private static final Set<String> V1_PACKAGES = Set.of("io.datahubproject.openapi.v1");
  private static final Set<String> V2_PACKAGES = Set.of("io.datahubproject.openapi.v2");
  private static final Set<String> V3_PACKAGES = Set.of("io.datahubproject.openapi.v3");
  private static final Set<String> SCHEMA_REGISTRY_PACKAGES =
      Set.of("io.datahubproject.openapi.schema.registry");

  public static final Set<String> NONDEFAULT_OPENAPI_PACKAGES;

  static {
    NONDEFAULT_OPENAPI_PACKAGES = new HashSet<>();
    NONDEFAULT_OPENAPI_PACKAGES.addAll(OPERATIONS_PACKAGES);
    NONDEFAULT_OPENAPI_PACKAGES.addAll(SCHEMA_REGISTRY_PACKAGES);
    NONDEFAULT_OPENAPI_PACKAGES.addAll(V1_PACKAGES);
    NONDEFAULT_OPENAPI_PACKAGES.addAll(V2_PACKAGES);
    NONDEFAULT_OPENAPI_PACKAGES.addAll(V3_PACKAGES);
  }

  @Override
  public void configureMessageConverters(List<HttpMessageConverter<?>> messageConverters) {
    messageConverters.add(new StringHttpMessageConverter());
    messageConverters.add(new ByteArrayHttpMessageConverter());
    messageConverters.add(new FormHttpMessageConverter());
    messageConverters.add(new MappingJackson2HttpMessageConverter());
  }

  @Override
  public void addFormatters(FormatterRegistry registry) {
    registry.addConverter(new StringToChangeCategoryConverter());
  }

  @Bean
  public GroupedOpenApi defaultOpenApiGroup() {
    return GroupedOpenApi.builder()
        .group("default")
        .packagesToExclude(NONDEFAULT_OPENAPI_PACKAGES.toArray(String[]::new))
        .build();
  }

  @Bean
  public GroupedOpenApi operationsOpenApiGroup() {
    return GroupedOpenApi.builder()
        .group("Operations")
        .packagesToScan(OPERATIONS_PACKAGES.toArray(String[]::new))
        .build();
  }

  @Bean
  public GroupedOpenApi openApiGroupV2() {
    return GroupedOpenApi.builder()
        .group("DataHub v2 (OpenAPI)")
        .packagesToScan(V2_PACKAGES.toArray(String[]::new))
        .build();
  }

  @Bean
  public GroupedOpenApi v3OpenApiGroup(final EntityRegistry entityRegistry) {
    return GroupedOpenApi.builder()
        .group("DataHub v3 (OpenAPI)")
        .addOpenApiCustomizer(
            openApi -> {
              OpenAPI v3OpenApi = OpenAPIV3Generator.generateOpenApiSpec(entityRegistry);
              openApi.setInfo(v3OpenApi.getInfo());
              openApi.setTags(Collections.emptyList());
              openApi.setPaths(v3OpenApi.getPaths());
              openApi.setComponents(v3OpenApi.getComponents());
            })
        .packagesToScan(V3_PACKAGES.toArray(String[]::new))
        .build();
  }
}
