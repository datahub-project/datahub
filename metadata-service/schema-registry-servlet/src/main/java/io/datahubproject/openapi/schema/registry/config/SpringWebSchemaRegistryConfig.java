package io.datahubproject.openapi.schema.registry.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.kafka.schemaregistry.InternalSchemaRegistryFactory;
import com.linkedin.metadata.registry.SchemaRegistryService;
import io.datahubproject.openapi.schema.registry.SchemaRegistryController;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@Slf4j
@EnableWebMvc
@Order(3)
@ConditionalOnProperty(
    name = "kafka.schemaRegistry.type",
    havingValue = InternalSchemaRegistryFactory.TYPE)
@Configuration
@ComponentScan(basePackages = {"io.datahubproject.openapi.schema.registry"})
public class SpringWebSchemaRegistryConfig {

  @Bean
  public SchemaRegistryController schemaRegistryController(
      ObjectMapper objectMapper,
      HttpServletRequest request,
      @Qualifier("schemaRegistryService") SchemaRegistryService schemaRegistryService) {
    return new SchemaRegistryController(objectMapper, request, schemaRegistryService);
  }
}
