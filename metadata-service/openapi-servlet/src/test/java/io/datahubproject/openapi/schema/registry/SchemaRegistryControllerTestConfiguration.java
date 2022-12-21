package io.datahubproject.openapi.schema.registry;

import com.linkedin.gms.factory.kafka.schemaregistry.InternalSchemaRegistryFactory;
import com.linkedin.metadata.config.KafkaConfiguration;
import com.linkedin.metadata.config.ProducerConfiguration;
import com.linkedin.metadata.config.SchemaRegistryConfiguration;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Scope;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;


@TestConfiguration
@ComponentScan(basePackages={"com.linkedin.gms.factory.kafka", "com.linkedin.gms.factory.config"})
public class SchemaRegistryControllerTestConfiguration {

  @Bean(name = "kafkaContainer")
  @Scope("singleton")
  public KafkaContainer kafkaContainer() {
    final KafkaContainer container = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.3.1"))
        .withStartupAttempts(5)
        .withStartupTimeout(Duration.of(30, ChronoUnit.SECONDS));
    container.start();
    return container;
  }

  @Bean(name = "kafka")
  @Scope("singleton")
  @Nonnull
  public KafkaConfiguration kafka(@Qualifier("kafkaContainer") KafkaContainer container,
      @Qualifier("testRestTemplate") TestRestTemplate _template) {
    final SchemaRegistryConfiguration schemaRegistryConfiguration = new SchemaRegistryConfiguration(
        InternalSchemaRegistryFactory.TYPE,
        String.format("%s/schema-registry", _template.getRootUri())
    );

    final ProducerConfiguration producerConfiguration = new ProducerConfiguration(
        3, 15000, 5000, 3000);

    return new KafkaConfiguration(container.getBootstrapServers(), schemaRegistryConfiguration, producerConfiguration);
  }
}
