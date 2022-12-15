package io.datahubproject.openapi.schema.registry;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import io.datahubproject.openapi.test.OpenAPISpringTestServer;
import io.datahubproject.openapi.test.OpenAPISpringTestServerConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


//TODO: Point to internal application.yaml with internal registry configs
//@TestPropertySource(locations = "classpath:/application-test.yml")
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    classes = {OpenAPISpringTestServer.class, OpenAPISpringTestServerConfiguration.class})
public class SchemaRegistryControllerTest extends AbstractTestNGSpringContextTests {

  // can we get spring boot application port from here????
  @Autowired
  private TestRestTemplate restTemplate;

  //@Autowired
  //EventProducer _producer;

  @Autowired
  SchemaRegistryController _controller;

  private static KafkaContainer kafkaContainer;

  @Autowired
  ConfigurationProvider _provider;

  @BeforeClass
  public void confluentSetup() {
    kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.3.1"));
    kafkaContainer.start();
    _provider.getKafka().setBootstrapServers(kafkaContainer.getBootstrapServers());
  }

  @Test
  public void testConsumption() {
    assert _controller != null;
  }
}