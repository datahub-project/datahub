package io.datahubproject.openapi.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.datahubproject.openapi.converter.StringToChangeCategoryConverter;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.servers.Server;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.directory.scim.core.json.ObjectMapperFactory;
import org.apache.directory.scim.core.schema.SchemaRegistry;
import org.apache.directory.scim.protocol.Constants;
import org.springdoc.core.models.GroupedOpenApi;
import org.springframework.beans.factory.annotation.Autowired;
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
  private static final Set<String> V2_PACKAGES = Set.of("io.datahubproject.openapi.v2");
  private static final Set<String> SCHEMA_REGISTRY_PACKAGES =
      Set.of("io.datahubproject.openapi.schema.registry");
  private static final Set<String> METADATA_TESTS_PACKAGES =
      Set.of("io.datahubproject.openapi.metadatatests");

  private static final Set<String> OPENLINEAGE_PACKAGES =
      Set.of("io.datahubproject.openapi.openlineage");

  public static final Set<String> NONDEFAULT_OPENAPI_PACKAGES;

  @Autowired SchemaRegistry schemaRegistry;

  static {
    NONDEFAULT_OPENAPI_PACKAGES = new HashSet<>();
    NONDEFAULT_OPENAPI_PACKAGES.addAll(OPERATIONS_PACKAGES);
    NONDEFAULT_OPENAPI_PACKAGES.addAll(V2_PACKAGES);
    NONDEFAULT_OPENAPI_PACKAGES.addAll(SCHEMA_REGISTRY_PACKAGES);
    NONDEFAULT_OPENAPI_PACKAGES.addAll(OPENLINEAGE_PACKAGES);
    NONDEFAULT_OPENAPI_PACKAGES.addAll(METADATA_TESTS_PACKAGES);
  }

  @Bean
  public MappingJackson2HttpMessageConverter jsonMessageConverter(SchemaRegistry schemaRegistry) {
    ObjectMapper objectMapper = ObjectMapperFactory.createObjectMapper(schemaRegistry);
    MappingJackson2HttpMessageConverter converter =
        new MappingJackson2HttpMessageConverter(objectMapper);
    converter.setSupportedMediaTypes(
        Arrays.asList(
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_XML,
            MediaType.valueOf(Constants.SCIM_CONTENT_TYPE)));
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

  //  @Override
  //  public void configureContentNegotiation(ContentNegotiationConfigurer configurer) {
  //    configurer
  //        .favorPathExtension(false)
  //        .favorParameter(true)
  //        .parameterName("format")
  //        .ignoreAcceptHeader(true)
  //        .useRegisteredExtensionsOnly(true)
  //        .mediaType("json", MediaType.APPLICATION_JSON)
  //        .mediaType("xml", MediaType.APPLICATION_XML)
  //        .mediaType("scim+json", MediaType.APPLICATION_JSON)
  //        .defaultContentType(MediaType.valueOf(Constants.SCIM_CONTENT_TYPE));
  //  }

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
  public GroupedOpenApi openApiGroupV3() {
    return GroupedOpenApi.builder()
        .group("OpenAPI v2")
        .packagesToScan(V2_PACKAGES.toArray(String[]::new))
        .build();
  }

  @Bean
  public GroupedOpenApi openlineageOpenApiGroup() {
    return GroupedOpenApi.builder()
        .group("openlineage")
        .packagesToScan(OPENLINEAGE_PACKAGES.toArray(String[]::new))
        .build();
  }

  @Bean
  public GroupedOpenApi metadataTestsOpenApiGroup() {
    return GroupedOpenApi.builder()
        .group("Metadata Tests")
        .packagesToScan(METADATA_TESTS_PACKAGES.toArray(String[]::new))
        .build();
  }
}
