package io.datahubproject.openapi.schema.registry.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.kafka.schemaregistry.InternalSchemaRegistryFactory;
import com.linkedin.metadata.registry.SchemaRegistryService;
import io.datahubproject.openapi.schema.registry.SchemaRegistryController;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.servers.Server;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Slf4j
@EnableWebMvc
@OpenAPIDefinition(
    info = @Info(title = "DataHub OpenAPI", version = "1.0.0"),
    servers = {@Server(url = "/schema-registry/", description = "Schema Registry Server URL")})
@Order(3)
@ConditionalOnProperty(
    name = "kafka.schemaRegistry.type",
    havingValue = InternalSchemaRegistryFactory.TYPE)
@Configuration
@ComponentScan(basePackages = {"io.datahubproject.openapi.schema.registry"})
public class SpringWebSchemaRegistryConfig implements WebMvcConfigurer {

  @Bean
  public SchemaRegistryController schemaRegistryController(
      ObjectMapper objectMapper,
      HttpServletRequest request,
      @Qualifier("schemaRegistryService") SchemaRegistryService schemaRegistryService) {
    return new SchemaRegistryController(objectMapper, request, schemaRegistryService);
  }

  @Override
  public void configureMessageConverters(List<HttpMessageConverter<?>> messageConverters) {
    messageConverters.add(new StringHttpMessageConverter());
    messageConverters.add(new ByteArrayHttpMessageConverter());
    messageConverters.add(new FormHttpMessageConverter());
    messageConverters.add(new MappingJackson2HttpMessageConverter());
  }
}
