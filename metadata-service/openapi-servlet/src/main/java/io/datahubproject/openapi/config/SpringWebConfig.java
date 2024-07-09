package io.datahubproject.openapi.config;

import static com.linkedin.metadata.Constants.INGESTION_MAX_SERIALIZED_STRING_LENGTH;
import static com.linkedin.metadata.Constants.MAX_JACKSON_STRING_SIZE;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.openapi.converter.StringToChangeCategoryConverter;
import io.datahubproject.openapi.v3.OpenAPIV3Generator;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.servers.Server;
import io.swagger.v3.oas.models.OpenAPI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.directory.scim.core.json.ObjectMapperFactory;
import org.apache.directory.scim.core.schema.SchemaRegistry;
import org.apache.directory.scim.protocol.Constants;
import org.springdoc.core.models.GroupedOpenApi;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.format.FormatterRegistry;
import org.springframework.http.MediaType;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.http.converter.xml.MappingJackson2XmlHttpMessageConverter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@OpenAPIDefinition(
    info = @Info(title = "DataHub OpenAPI", version = "2.0.0"),
    servers = {@Server(url = "/openapi/", description = "Default Server URL")})
@Configuration
public class SpringWebConfig implements WebMvcConfigurer {
  private static final Set<String> OPERATIONS_PACKAGES =
      Set.of(
          "io.datahubproject.openapi.operations",
          "io.datahubproject.openapi.health",
          "io.datahubproject.openapi.tests");
  private static final Set<String> V1_PACKAGES = Set.of("io.datahubproject.openapi.v1");
  private static final Set<String> V2_PACKAGES = Set.of("io.datahubproject.openapi.v2");
  private static final Set<String> V3_PACKAGES = Set.of("io.datahubproject.openapi.v3");
  private static final Set<String> SCHEMA_REGISTRY_PACKAGES =
      Set.of("io.datahubproject.openapi.schema.registry");
  private static final Set<String> METADATA_TESTS_PACKAGES =
      Set.of("io.datahubproject.openapi.metadatatests");

  private static final Set<String> OPENLINEAGE_PACKAGES =
      Set.of("io.datahubproject.openapi.openlineage");

  @Autowired SchemaRegistry schemaRegistry;

  @Bean
  public MappingJackson2HttpMessageConverter jsonMessageConverter(SchemaRegistry schemaRegistry) {
    ObjectMapper objectMapper = ObjectMapperFactory.createObjectMapper(schemaRegistry);
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    objectMapper
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
    objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    MappingJackson2HttpMessageConverter converter =
        new MappingJackson2HttpMessageConverter(objectMapper);
    converter.setSupportedMediaTypes(
        Arrays.asList(
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_XML,
            MediaType.valueOf(Constants.SCIM_CONTENT_TYPE),
            MediaType.valueOf("application/json-patch+json")));
    return converter;
  }

  @Bean
  public MappingJackson2XmlHttpMessageConverter xmlMessageConverter(SchemaRegistry schemaRegistry) {
    ObjectMapper objectMapper = ObjectMapperFactory.createXmlObjectMapper(schemaRegistry);
    MappingJackson2XmlHttpMessageConverter converter =
        new MappingJackson2XmlHttpMessageConverter(objectMapper);
    converter.setSupportedMediaTypes(
        Arrays.asList(
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_XML,
            MediaType.valueOf(Constants.SCIM_CONTENT_TYPE)));
    return converter;
  }

  @Override
  public void configureMessageConverters(List<HttpMessageConverter<?>> messageConverters) {
    messageConverters.add(new StringHttpMessageConverter());
    messageConverters.add(new ByteArrayHttpMessageConverter());
    messageConverters.add(new FormHttpMessageConverter());
    messageConverters.add(jsonMessageConverter(schemaRegistry));
    messageConverters.add(xmlMessageConverter(schemaRegistry));
  }

  @Override
  public void extendMessageConverters(List<HttpMessageConverter<?>> converters) {
    converters.add(jsonMessageConverter(schemaRegistry));
    converters.add(xmlMessageConverter(schemaRegistry));
  }

  @Override
  public void addFormatters(FormatterRegistry registry) {
    registry.addConverter(new StringToChangeCategoryConverter());
  }

  @Bean
  public GroupedOpenApi v3OpenApiGroup(final EntityRegistry entityRegistry) {
    return GroupedOpenApi.builder()
        .group("10-openapi-v3")
        .displayName("DataHub Entities v3 (OpenAPI)")
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

  @Bean
  @ConditionalOnProperty(name = "metadataTests.enabled", havingValue = "true")
  public GroupedOpenApi metadataTestsOpenApiGroup() {
    return GroupedOpenApi.builder()
        .group("60-metadatatests")
        .displayName("Metadata Tests")
        .packagesToScan(METADATA_TESTS_PACKAGES.toArray(String[]::new))
        .build();
  }
}
