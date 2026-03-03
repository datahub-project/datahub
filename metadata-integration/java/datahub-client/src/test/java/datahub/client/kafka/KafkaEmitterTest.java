package datahub.client.kafka;

import static datahub.client.kafka.KafkaEmitter.DEFAULT_MCP_KAFKA_TOPIC;
import static java.util.Collections.singletonList;

import com.linkedin.dataset.DatasetProperties;
import datahub.client.MetadataWriteResponse;
import datahub.client.kafka.containers.KafkaContainer;
import datahub.client.kafka.containers.SchemaRegistryContainer;
import datahub.event.MetadataChangeProposalWrapper;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.avro.Schema;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.Network;
import org.testng.Assert;

public class KafkaEmitterTest {

  private static final String TOPIC = DEFAULT_MCP_KAFKA_TOPIC;

  private static Network network;

  private static KafkaContainer kafkaContainer;
  private static SchemaRegistryContainer schemaRegistryContainer;
  private static KafkaEmitterConfig config;
  private static KafkaEmitter emitter;

  @SuppressWarnings("resource")
  @BeforeClass
  public static void confluentSetup() throws Exception {
    try {
      network = Network.newNetwork();

      // Start Kafka with KRaft (no Zookeeper needed)
      kafkaContainer = new KafkaContainer().withNetwork(network);
      kafkaContainer.start();

      // Schema Registry now only depends on Kafka
      schemaRegistryContainer =
          new SchemaRegistryContainer(kafkaContainer.getInternalBootstrapServers())
              .withNetwork(network)
              .dependsOn(kafkaContainer);
      schemaRegistryContainer.start();

      createTopics(kafkaContainer.getBootstrapServers());
      createKafkaEmitter(kafkaContainer.getBootstrapServers());
      registerSchemaRegistryTypes();
    } catch (IllegalStateException e) {
      if (e.getMessage() != null
          && e.getMessage().contains("Could not find a valid Docker environment")) {
        Assume.assumeTrue(
            "Docker/Testcontainers not available (e.g. CI without Docker or API too old)", false);
      }
      throw e;
    }
  }

  public static void createKafkaEmitter(String bootstrap) throws IOException {
    KafkaEmitterConfig.KafkaEmitterConfigBuilder builder = KafkaEmitterConfig.builder();
    builder.bootstrap(bootstrap);
    builder.schemaRegistryUrl(schemaRegistryContainer.getUrl());
    config = builder.build();
    emitter = new KafkaEmitter(config);
  }

  @Test
  public void testConnection() throws IOException, ExecutionException, InterruptedException {
    Assert.assertTrue(emitter.testConnection());
  }

  @Test
  public void testSend() throws IOException, InterruptedException, ExecutionException {

    @SuppressWarnings("rawtypes")
    MetadataChangeProposalWrapper mcpw =
        getMetadataChangeProposalWrapper(
            "Test Dataset", "urn:li:dataset:(urn:li:dataPlatform:spark,foo.bar,PROD)");
    Future<MetadataWriteResponse> future = emitter.emit(mcpw);
    MetadataWriteResponse response = future.get();
    System.out.println("Response: " + response);
    Assert.assertTrue(response.isSuccess());
  }

  private static AdminClient createAdminClient(String bootstrap) {
    // Fail fast
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    return KafkaAdminClient.create(props);
  }

  private static void registerSchemaRegistryTypes() throws IOException, RestClientException {
    Schema mcpSchema = new AvroSerializer().getRecordSchema();
    CachedSchemaRegistryClient schemaRegistryClient =
        new CachedSchemaRegistryClient(schemaRegistryContainer.getUrl(), 1000);
    schemaRegistryClient.register(mcpSchema.getFullName(), mcpSchema);
  }

  private static void createTopics(String bootstrap)
      throws ExecutionException, InterruptedException {
    short replicationFactor = 1;
    int partitions = 1;

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    AdminClient adminClient = KafkaAdminClient.create(props);

    adminClient
        .createTopics(singletonList(new NewTopic(TOPIC, partitions, replicationFactor)))
        .all()
        .get();
  }

  @SuppressWarnings("rawtypes")
  private MetadataChangeProposalWrapper getMetadataChangeProposalWrapper(
      String description, String entityUrn) {
    return MetadataChangeProposalWrapper.builder()
        .entityType("dataset")
        .entityUrn(entityUrn)
        .upsert()
        .aspect(new DatasetProperties().setDescription(description))
        .build();
  }
}
