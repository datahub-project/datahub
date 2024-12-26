package io.datahubproject.openapi.config;

import static com.linkedin.metadata.Constants.INGESTION_MAX_SERIALIZED_STRING_LENGTH;
import static com.linkedin.metadata.Constants.MAX_JACKSON_STRING_SIZE;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.openapi.converter.StringToChangeCategoryConverter;
import io.datahubproject.openapi.v3.OpenAPIV3Generator;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.servers.Server;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.springdoc.core.models.GroupedOpenApi;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.format.FormatterRegistry;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

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

  private static final Set<String> OPENLINEAGE_PACKAGES =
      Set.of("io.datahubproject.openapi.openlineage");

  @Value("${datahub.gms.async.request-timeout-ms}")
  private long asyncTimeoutMilliseconds;

  @Override
  public void configureMessageConverters(List<HttpMessageConverter<?>> messageConverters) {
    messageConverters.add(new StringHttpMessageConverter());
    messageConverters.add(new ByteArrayHttpMessageConverter());
    messageConverters.add(new FormHttpMessageConverter());

    ObjectMapper objectMapper = new ObjectMapper();
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    objectMapper
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
    objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    MappingJackson2HttpMessageConverter jsonConverter =
        new MappingJackson2HttpMessageConverter(objectMapper);
    messageConverters.add(jsonConverter);
  }

  @Override
  public void addFormatters(FormatterRegistry registry) {
    registry.addConverter(new StringToChangeCategoryConverter());
  }

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

  @Override
  public void configureAsyncSupport(@Nonnull AsyncSupportConfigurer configurer) {
    WebMvcConfigurer.super.configureAsyncSupport(configurer);
    configurer.setDefaultTimeout(asyncTimeoutMilliseconds);
  }
}
