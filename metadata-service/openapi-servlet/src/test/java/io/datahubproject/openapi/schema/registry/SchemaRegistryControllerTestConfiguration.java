package io.datahubproject.openapi.schema.registry;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.TestPropertySource;


@TestConfiguration
@TestPropertySource(value = "classpath:/application.properties")
@ComponentScan(basePackages = {"com.linkedin.gms.factory.kafka", "com.linkedin.gms.factory.config"})
public class SchemaRegistryControllerTestConfiguration {

}
