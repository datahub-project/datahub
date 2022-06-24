package datahub.client.kafka;

import static datahub.client.kafka.KafkaEmitter.DEFAULT_MCP_KAFKA_TOPIC;
import static java.util.Collections.singletonList;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;
import org.testng.Assert;

import com.linkedin.dataset.DatasetProperties;

import datahub.client.MetadataWriteResponse;
import datahub.client.kafka.containers.KafkaContainer;
import datahub.client.kafka.containers.SchemaRegistryContainer;
import datahub.client.kafka.containers.ZookeeperContainer;
import datahub.event.MetadataChangeProposalWrapper;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

public class KafkaEmitterTest {

  private static final String TOPIC = DEFAULT_MCP_KAFKA_TOPIC;

  private static Network network;

  private static ZookeeperContainer zookeeperContainer;
  private static KafkaContainer kafkaContainer;
  private static SchemaRegistryContainer schemaRegistryContainer;
  private KafkaEmitterConfig config;
  private KafkaEmitter emitter;

  @SuppressWarnings("resource")
  @Before
  public void confluentSetup() throws Exception {
    network = Network.newNetwork();
    zookeeperContainer = new ZookeeperContainer().withNetwork(network);
    kafkaContainer = new KafkaContainer(zookeeperContainer.getInternalUrl()).withNetwork(network);
    schemaRegistryContainer = new SchemaRegistryContainer(zookeeperContainer.getInternalUrl()).withNetwork(network);
    Startables.deepStart(Stream.of(zookeeperContainer, kafkaContainer, schemaRegistryContainer)).join();

    createKafkaEmitter();
    createTopics();
    registerSchemaRegistryTypes();

  }

  public void createKafkaEmitter() throws IOException {
    KafkaEmitterConfig.KafkaEmitterConfigBuilder builder = KafkaEmitterConfig.builder();
    builder.bootstrap(kafkaContainer.getBootstrapServers());
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
    MetadataChangeProposalWrapper mcpw = getMetadataChangeProposalWrapper("Test Dataset",
        "urn:li:dataset:(urn:li:dataPlatform:spark,foo.bar,PROD)");
    Future<MetadataWriteResponse> future = emitter.emit(mcpw);
    MetadataWriteResponse response = future.get();
    System.out.println("Response: " + response);
    Assert.assertTrue(response.isSuccess());
  }

  private AdminClient createAdminClient() {
    return KafkaAdminClient.create(emitter.getKafkaConfgiProperties());
  }

  private static void registerSchemaRegistryTypes() throws IOException, RestClientException {
    Schema mcpSchema = new AvroSerializer().getRecordSchema();
    CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryContainer.getUrl(), 1000);
    schemaRegistryClient.register(mcpSchema.getFullName(), mcpSchema);
  }

  private void createTopics() throws InterruptedException, ExecutionException {
    AdminClient adminClient = createAdminClient();
    short replicationFactor = 1;
    int partitions = 1;
    adminClient.createTopics(singletonList(new NewTopic(TOPIC, partitions, replicationFactor))).all().get();
  }

  @SuppressWarnings("rawtypes")
  private MetadataChangeProposalWrapper getMetadataChangeProposalWrapper(String description, String entityUrn) {
    return MetadataChangeProposalWrapper.builder().entityType("dataset").entityUrn(entityUrn).upsert()
        .aspect(new DatasetProperties().setDescription(description)).build();
  }
}