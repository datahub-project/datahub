package io.datahubproject.openapi.test;

import com.linkedin.gms.factory.kafka.schemaregistry.InternalSchemaRegistryFactory;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.config.KafkaConfiguration;
import com.linkedin.metadata.config.SchemaRegistryConfiguration;
import com.linkedin.mxe.Topics;
import javax.annotation.Nonnull;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.servlet.DispatcherServlet;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;


@TestConfiguration
@ComponentScan(basePackages={"io.datahubproject.openapi.schema.registry", "com.linkedin.metadata.schema.registry",
    "com.linkedin.gms.factory.kafka", "com.linkedin.gms.factory.config", "io.datahubproject.openapi.schema.registry"})
public class OpenAPISpringTestServerConfiguration {

  @LocalServerPort
  private int port;

  @Bean(name = "testRestTemplate")
  public TestRestTemplate testRestTemplate() {
    var restTemplate = new RestTemplateBuilder().rootUri("http://localhost:" + port);
    return new TestRestTemplate(restTemplate);
  }

  @Bean
  public DispatcherServlet dispatcherServlet() {
    return new DispatcherServlet();
  }

  @Bean(name = "kafkaContainer")
  @Scope("singleton")
  public KafkaContainer kafkaContainer() {
    final KafkaContainer container = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.3.1"));
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

    return new KafkaConfiguration(container.getBootstrapServers(), schemaRegistryConfiguration);
  }

  @Bean
  public ServletRegistrationBean<DispatcherServlet> servletRegistrationBean(DispatcherServlet dispatcherServlet) {
    return new ServletRegistrationBean<>(dispatcherServlet, "/");
  }

  @KafkaListener(id = "${METADATA_CHANGE_EVENT_KAFKA_CONSUMER_GROUP_ID:mce-consumer-job-client-v2}", topics =
      "${METADATA_CHANGE_EVENT_NAME:${KAFKA_MCE_TOPIC_NAME:" + Topics.METADATA_CHANGE_EVENT + "}}", containerFactory = "kafkaEventConsumer")
  public void consume_mce(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    final GenericRecord record = consumerRecord.value();

    try {
      EventUtils.avroToPegasusMCE(record);
    } catch (Throwable ignored) {
    }
  }

  @KafkaListener(id = "${METADATA_CHANGE_PROPOSAL_KAFKA_CONSUMER_GROUP_ID:generic-mce-consumer-job-client-v2}", topics =
      "${METADATA_CHANGE_PROPOSAL_TOPIC_NAME:" + Topics.METADATA_CHANGE_PROPOSAL + "}", containerFactory = "kafkaEventConsumer")
  public void consume_mcp(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    final GenericRecord record = consumerRecord.value();

    try {
      EventUtils.avroToPegasusMCP(record);
    } catch (Throwable ignored) {
    }
  }
}
