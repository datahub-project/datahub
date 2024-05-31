package io.datahubproject.openapi.test;

import com.linkedin.metadata.dao.producer.KafkaHealthChecker;
import com.linkedin.metadata.models.registry.EntityRegistry;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.TestPropertySource;

@TestConfiguration
@TestPropertySource(value = "classpath:/application.properties")
@ComponentScan(basePackages = {"com.linkedin.gms.factory.kafka", "com.linkedin.gms.factory.config"})
public class SchemaRegistryControllerTestConfiguration {
  @MockBean KafkaHealthChecker kafkaHealthChecker;

  @MockBean EntityRegistry entityRegistry;
}
