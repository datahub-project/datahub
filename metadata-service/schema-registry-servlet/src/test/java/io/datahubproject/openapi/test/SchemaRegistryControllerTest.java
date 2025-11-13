package io.datahubproject.openapi.test;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.config.kafka.KafkaConfiguration.DEFAULT_EVENT_CONSUMER_NAME;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.JacksonDataTemplateCodec;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.FailedMetadataChangeProposal;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.PlatformEventHeader;
import com.linkedin.mxe.Topics;
import com.linkedin.platform.event.v1.EntityChangeEvent;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import mock.MockEntitySpec;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.Test;

@Slf4j
@ActiveProfiles("test")
@ContextConfiguration
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
    classes = {
      OpenAPISpringTestServer.class,
      OpenAPISpringTestServerConfiguration.class,
      SchemaRegistryControllerTestConfiguration.class
    })
@EnableKafka
@TestPropertySource(properties = {"kafka.consumer.stopOnDeserializationError=false"})
public class SchemaRegistryControllerTest extends AbstractTestNGSpringContextTests {
  private static final String CONFLUENT_PLATFORM_VERSION = "7.4.10";

  static KafkaContainer kafka =
      new KafkaContainer(
              DockerImageName.parse("confluentinc/cp-kafka:" + CONFLUENT_PLATFORM_VERSION))
          .withReuse(true)
          .withStartupAttempts(5)
          .withStartupTimeout(Duration.of(30, ChronoUnit.SECONDS));

  @DynamicPropertySource
  static void kafkaProperties(DynamicPropertyRegistry registry) {
    kafka.start();
    registry.add("kafka.bootstrapServers", kafka::getBootstrapServers);
    registry.add("kafka.schemaRegistry.type", () -> "INTERNAL");
    registry.add("kafka.schemaRegistry.url", () -> "http://localhost:53222/schema-registry/api/");
  }

  @Autowired EventProducer _producer;

  private final Map<String, CountDownLatch> latches = new ConcurrentHashMap<>();
  private final Map<String, AtomicReference<Object>> references = new ConcurrentHashMap<>();

  // Helper methods to get latch and reference for a message key
  private CountDownLatch getLatch(String messageKey) {
    return latches.computeIfAbsent(messageKey, k -> new CountDownLatch(1));
  }

  @SuppressWarnings("unchecked")
  private <T> AtomicReference<T> getReference(String messageKey) {
    return (AtomicReference<T>)
        references.computeIfAbsent(messageKey, k -> new AtomicReference<>());
  }

  @Test
  public void testMCPConsumption()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    final Urn entityUrn =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,testMCPConsumption,PROD)");
    final DatasetProperties datasetProperties = new DatasetProperties();
    datasetProperties.setName("Foo Bar");

    final MetadataChangeProposal gmce = new MetadataChangeProposal();
    gmce.setEntityUrn(entityUrn);
    gmce.setChangeType(ChangeType.UPSERT);
    gmce.setEntityType("dataset");
    gmce.setAspectName("datasetProperties");

    final JacksonDataTemplateCodec dataTemplateCodec = new JacksonDataTemplateCodec();
    final byte[] datasetPropertiesSerialized =
        dataTemplateCodec.dataTemplateToBytes(datasetProperties);
    final GenericAspect genericAspect = new GenericAspect();
    genericAspect.setValue(ByteString.unsafeWrap(datasetPropertiesSerialized));
    genericAspect.setContentType("application/json");
    gmce.setAspect(genericAspect);

    _producer.produceMetadataChangeProposal(entityUrn, gmce).get(10, TimeUnit.SECONDS);
    // Wait for message to be consumed and deserialized
    final boolean messageConsumed = getLatch(entityUrn.toString()).await(10, TimeUnit.SECONDS);
    assertTrue(messageConsumed);
    assertEquals(getLatch(entityUrn.toString()).getCount(), 0);
    assertEquals(getReference(entityUrn.toString()).get(), gmce);
  }

  @Test
  public void testMCLConsumption()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    final Urn entityUrn =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,testMCLConsumption,PROD)");
    final DatasetProperties datasetProperties = new DatasetProperties();
    datasetProperties.setName("Bar Baz");
    final JacksonDataTemplateCodec dataTemplateCodec = new JacksonDataTemplateCodec();

    final MetadataChangeLog metadataChangeLog = new MetadataChangeLog();
    metadataChangeLog.setEntityType("dataset");
    metadataChangeLog.setEntityUrn(entityUrn);
    metadataChangeLog.setChangeType(ChangeType.UPSERT);
    metadataChangeLog.setAspectName("datasetProperties");
    metadataChangeLog.setCreated(AuditStampUtils.createDefaultAuditStamp());

    // Set old aspect
    final GenericAspect oldAspect = new GenericAspect();
    final byte[] oldDatasetPropertiesSerialized =
        dataTemplateCodec.dataTemplateToBytes(datasetProperties);
    oldAspect.setValue(ByteString.unsafeWrap(oldDatasetPropertiesSerialized));
    oldAspect.setContentType("application/json");
    metadataChangeLog.setPreviousAspectValue(GenericRecordUtils.serializeAspect(oldAspect));
    metadataChangeLog.setPreviousSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata());

    // Set new aspect
    final GenericAspect newAspectValue = new GenericAspect();
    datasetProperties.setDescription("Updated data");
    final byte[] newDatasetPropertiesSerialized =
        dataTemplateCodec.dataTemplateToBytes(datasetProperties);
    newAspectValue.setValue(ByteString.unsafeWrap(newDatasetPropertiesSerialized));
    newAspectValue.setContentType("application/json");
    metadataChangeLog.setAspect(GenericRecordUtils.serializeAspect(newAspectValue));
    metadataChangeLog.setSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata());

    final MockEntitySpec entitySpec = new MockEntitySpec("dataset");
    final AspectSpec aspectSpec =
        entitySpec.createAspectSpec(datasetProperties, DATASET_PROPERTIES_ASPECT_NAME);

    _producer
        .produceMetadataChangeLog(entityUrn, aspectSpec, metadataChangeLog)
        .get(10, TimeUnit.SECONDS);
    final boolean messageConsumed = getLatch(entityUrn.toString()).await(10, TimeUnit.SECONDS);
    assertTrue(messageConsumed);
    assertEquals(getLatch(entityUrn.toString()).getCount(), 0);
    assertEquals(getReference(entityUrn.toString()).get(), metadataChangeLog);
  }

  @Test
  public void testPEConsumption()
      throws InterruptedException, ExecutionException, TimeoutException {

    final Urn entityUrn =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,testPEConsumption,PROD)");
    final EntityChangeEvent changeEvent = new EntityChangeEvent();
    final ChangeCategory category = ChangeCategory.OWNER;
    final ChangeOperation operation = ChangeOperation.ADD;

    changeEvent.setEntityType(DATASET_ENTITY_NAME);
    changeEvent.setEntityUrn(entityUrn);
    changeEvent.setCategory(category.name());
    changeEvent.setOperation(operation.name());
    changeEvent.setAuditStamp(AuditStampUtils.createDefaultAuditStamp());
    changeEvent.setVersion(0);

    final PlatformEvent platformEvent = new PlatformEvent();
    platformEvent.setName(CHANGE_EVENT_PLATFORM_EVENT_NAME);
    platformEvent.setHeader(new PlatformEventHeader().setTimestampMillis(123L));
    platformEvent.setPayload(GenericRecordUtils.serializePayload(changeEvent));

    _producer
        .producePlatformEvent(CHANGE_EVENT_PLATFORM_EVENT_NAME, "testPEConsumption", platformEvent)
        .get(10, TimeUnit.SECONDS);

    final boolean messageConsumed = getLatch("testPEConsumption").await(10, TimeUnit.SECONDS);
    assertTrue(messageConsumed);
    assertEquals(getLatch("testPEConsumption").getCount(), 0);
    assertEquals(getReference("testPEConsumption").get(), platformEvent);
  }

  @Test
  public void testFailedMetadataChangeProposalRaw()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {

    // Load binary test fixture (raw bytes from local Kafka)
    byte[] rawMessageBytes =
        loadBinaryTestFixture("/v1/FailedMetadataChangeProposal_v1_test_fixture.bin");
    assertNotNull(rawMessageBytes, "Binary test fixture should be loaded");
    assertTrue(rawMessageBytes.length > 0, "Binary test fixture should not be empty");

    // Create a custom Kafka producer to publish raw bytes
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

    try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps)) {
      // Publish raw message bytes to Kafka with unique key for this test
      ProducerRecord<String, byte[]> record =
          new ProducerRecord<>(
              Topics.FAILED_METADATA_CHANGE_PROPOSAL,
              "testFailedMetadataChangeProposalRaw",
              rawMessageBytes);

      producer.send(record).get(10, TimeUnit.SECONDS);
      producer.flush();
    }

    // Wait for message to be consumed and deserialized
    final boolean messageConsumed =
        getLatch("testFailedMetadataChangeProposalRaw").await(10, TimeUnit.SECONDS);
    assertTrue(
        messageConsumed,
        "Raw FailedMetadataChangeProposal message should be consumed and deserialized");
    assertEquals(getLatch("testFailedMetadataChangeProposalRaw").getCount(), 0);

    // Verify the consumed message was properly deserialized
    MetadataChangeProposal consumedEvent =
        (MetadataChangeProposal) getReference("testFailedMetadataChangeProposalRaw").get();
    assertNotNull(
        consumedEvent, "Consumed event should not be null after raw message deserialization");

    // Verify the message structure is correct (this proves deserialization worked)
    assertNotNull(consumedEvent.getEntityUrn(), "EntityUrn should be deserialized correctly");
    assertEquals(consumedEvent.getEntityUrn().toString(), "urn:li:corpuser:datahub");
  }

  @Test
  public void testMetadataChangeLogTimeseriesRaw()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {

    // Load binary test fixture (raw bytes from local Kafka)
    byte[] rawMessageBytes =
        loadBinaryTestFixture("/v1/MetadataChangeLog_Timeseries_v1_test_fixture.bin");
    assertNotNull(rawMessageBytes, "Binary test fixture should be loaded");
    assertTrue(rawMessageBytes.length > 0, "Binary test fixture should not be empty");

    // Create a custom Kafka producer to publish raw bytes
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

    try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps)) {
      // Publish raw message bytes to Kafka
      ProducerRecord<String, byte[]> record =
          new ProducerRecord<>(
              Topics.METADATA_CHANGE_LOG_TIMESERIES,
              "testMetadataChangeLogTimeseriesRaw",
              rawMessageBytes);

      producer.send(record).get(10, TimeUnit.SECONDS);
      producer.flush();
    }

    // Wait for message to be consumed and deserialized
    final boolean messageConsumed =
        getLatch("testMetadataChangeLogTimeseriesRaw").await(10, TimeUnit.SECONDS);
    assertTrue(
        messageConsumed,
        "Raw MetadataChangeLog Timeseries message should be consumed and deserialized");
    assertEquals(getLatch("testMetadataChangeLogTimeseriesRaw").getCount(), 0);

    // Verify the consumed message was properly deserialized
    MetadataChangeLog consumedEvent =
        (MetadataChangeLog) getReference("testMetadataChangeLogTimeseriesRaw").get();
    assertNotNull(
        consumedEvent, "Consumed event should not be null after raw message deserialization");

    // Verify the message structure is correct (this proves deserialization worked)
    assertNotNull(consumedEvent.getEntityUrn(), "EntityUrn should be deserialized correctly");
    assertEquals(
        consumedEvent.getEntityUrn().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)");
  }

  @Test
  public void testMetadataChangeLogVersionedRaw()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {

    // Load binary test fixture (raw bytes from local Kafka)
    byte[] rawMessageBytes =
        loadBinaryTestFixture("/v1/MetadataChangeLog_Versioned_v1_test_fixture.bin");
    assertNotNull(rawMessageBytes, "Binary test fixture should be loaded");
    assertTrue(rawMessageBytes.length > 0, "Binary test fixture should not be empty");

    // Create a custom Kafka producer to publish raw bytes
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

    try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps)) {
      // Publish raw message bytes to Kafka
      ProducerRecord<String, byte[]> record =
          new ProducerRecord<>(
              Topics.METADATA_CHANGE_LOG_VERSIONED,
              "testMetadataChangeLogVersionedRaw",
              rawMessageBytes);

      producer.send(record).get(10, TimeUnit.SECONDS);
      producer.flush();
    }

    // Wait for message to be consumed and deserialized
    final boolean messageConsumed =
        getLatch("testMetadataChangeLogVersionedRaw").await(10, TimeUnit.SECONDS);
    assertTrue(
        messageConsumed,
        "Raw MetadataChangeLog Versioned message should be consumed and deserialized");
    assertEquals(getLatch("testMetadataChangeLogVersionedRaw").getCount(), 0);

    // Verify the consumed message was properly deserialized
    MetadataChangeLog consumedEvent =
        (MetadataChangeLog) getReference("testMetadataChangeLogVersionedRaw").get();
    assertNotNull(
        consumedEvent, "Consumed event should not be null after raw message deserialization");

    // Verify the message structure is correct (this proves deserialization worked)
    assertNotNull(consumedEvent.getEntityUrn(), "EntityUrn should be deserialized correctly");
    assertEquals(consumedEvent.getEntityUrn().toString(), "urn:li:corpuser:datahub");
  }

  @Test
  public void testMetadataChangeProposalRaw()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {

    // Load binary test fixture (raw bytes from local Kafka)
    byte[] rawMessageBytes =
        loadBinaryTestFixture("/v1/MetadataChangeProposal_v1_test_fixture.bin");
    assertNotNull(rawMessageBytes, "Binary test fixture should be loaded");
    assertTrue(rawMessageBytes.length > 0, "Binary test fixture should not be empty");

    // Create a custom Kafka producer to publish raw bytes
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

    try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps)) {
      // Publish raw message bytes to Kafka
      ProducerRecord<String, byte[]> record =
          new ProducerRecord<>(
              Topics.METADATA_CHANGE_PROPOSAL, "testMetadataChangeProposalRaw", rawMessageBytes);

      producer.send(record).get(10, TimeUnit.SECONDS);
      producer.flush();
    }

    // Wait for message to be consumed and deserialized
    final boolean messageConsumed =
        getLatch("testMetadataChangeProposalRaw").await(10, TimeUnit.SECONDS);
    assertTrue(
        messageConsumed, "Raw MetadataChangeProposal message should be consumed and deserialized");
    assertEquals(getLatch("testMetadataChangeProposalRaw").getCount(), 0);

    // Verify the consumed message was properly deserialized
    MetadataChangeProposal consumedEvent =
        (MetadataChangeProposal) getReference("testMetadataChangeProposalRaw").get();
    assertNotNull(
        consumedEvent, "Consumed event should not be null after raw message deserialization");

    // Verify the message structure is correct (this proves deserialization worked)
    assertNotNull(consumedEvent.getEntityUrn(), "EntityUrn should be deserialized correctly");
    assertEquals(
        consumedEvent.getEntityUrn().toString(), "urn:li:dataHubIngestionSource:datahub-gc");
  }

  private byte[] loadBinaryTestFixture(String fixturePath) throws IOException {
    try (InputStream inputStream = getClass().getResourceAsStream(fixturePath)) {
      if (inputStream == null) {
        throw new IOException("Binary test fixture not found: " + fixturePath);
      }
      return inputStream.readAllBytes();
    }
  }

  @KafkaListener(
      id = "test-mcp-consumer",
      topics = Topics.METADATA_CHANGE_PROPOSAL,
      containerFactory = DEFAULT_EVENT_CONSUMER_NAME,
      properties = {
        "auto.offset.reset:earliest",
        "spring.kafka.listener.ack-mode:manual",
        "spring.kafka.listener.ack-on-error:false",
        "spring.kafka.listener.retry-after-error:false",
        "spring.kafka.listener.fail-fast:false"
      })
  public void receiveMCP(ConsumerRecord<String, GenericRecord> consumerRecord) {

    final GenericRecord value = consumerRecord.value();
    try {
      String messageKey = consumerRecord.key();

      getReference(messageKey).set(EventUtils.avroToPegasusMCP(value));
      getLatch(messageKey).countDown();
    } catch (IOException e) {
      log.error(
          "Failed to deserialize MCP message with key: {}, error: {}",
          consumerRecord.key(),
          e.getMessage(),
          e);
      // Continue processing other messages instead of stopping the consumer
    }
  }

  @KafkaListener(
      id = "test-mcl-consumer",
      topics = Topics.METADATA_CHANGE_LOG_VERSIONED,
      containerFactory = DEFAULT_EVENT_CONSUMER_NAME,
      properties = {
        "auto.offset.reset:earliest",
        "spring.kafka.listener.ack-mode:manual",
        "spring.kafka.listener.ack-on-error:false",
        "spring.kafka.listener.retry-after-error:false",
        "spring.kafka.listener.fail-fast:false"
      })
  public void receiveMCL(ConsumerRecord<String, GenericRecord> consumerRecord) {

    final GenericRecord value = consumerRecord.value();
    try {
      String messageKey = consumerRecord.key();
      getReference(messageKey).set(EventUtils.avroToPegasusMCL(value));
      getLatch(messageKey).countDown();
    } catch (IOException e) {
      log.error(
          "Failed to deserialize MCL message with key: {}, error: {}",
          consumerRecord.key(),
          e.getMessage(),
          e);
      // Continue processing other messages instead of stopping the consumer
    }
  }

  @KafkaListener(
      id = "test-mcl-timeseries-consumer",
      topics = Topics.METADATA_CHANGE_LOG_TIMESERIES,
      containerFactory = DEFAULT_EVENT_CONSUMER_NAME,
      properties = {
        "auto.offset.reset:earliest",
        "spring.kafka.listener.ack-mode:manual",
        "spring.kafka.listener.ack-on-error:false",
        "spring.kafka.listener.retry-after-error:false",
        "spring.kafka.listener.fail-fast:false"
      })
  public void receiveMCLTimeseries(ConsumerRecord<String, GenericRecord> consumerRecord) {

    final GenericRecord value = consumerRecord.value();
    try {
      String messageKey = consumerRecord.key();
      getReference(messageKey).set(EventUtils.avroToPegasusMCL(value));
      getLatch(messageKey).countDown();
    } catch (IOException e) {
      log.error(
          "Failed to deserialize MCL timeseries message with key: {}, error: {}",
          consumerRecord.key(),
          e.getMessage(),
          e);
      // Continue processing other messages instead of stopping the consumer
    }
  }

  @KafkaListener(
      id = "test-pe-consumer",
      topics = Topics.PLATFORM_EVENT,
      containerFactory = DEFAULT_EVENT_CONSUMER_NAME,
      properties = {
        "auto.offset.reset:earliest",
        "spring.kafka.listener.ack-mode:manual",
        "spring.kafka.listener.ack-on-error:false",
        "spring.kafka.listener.retry-after-error:false",
        "spring.kafka.listener.fail-fast:false"
      })
  public void receivePE(ConsumerRecord<String, GenericRecord> consumerRecord) {

    final GenericRecord value = consumerRecord.value();
    try {
      String messageKey = consumerRecord.key();
      getReference(messageKey).set(EventUtils.avroToPegasusPE(value));
      getLatch(messageKey).countDown();
    } catch (IOException e) {
      log.error(
          "Failed to deserialize PE message with key: {}, error: {}",
          consumerRecord.key(),
          e.getMessage(),
          e);
      // Continue processing other messages instead of stopping the consumer
    }
  }

  @KafkaListener(
      id = "test-duhe-consumer",
      topics = Topics.DATAHUB_UPGRADE_HISTORY_TOPIC_NAME,
      containerFactory = DEFAULT_EVENT_CONSUMER_NAME,
      properties = {
        "auto.offset.reset:earliest",
        "spring.kafka.listener.ack-mode:manual",
        "spring.kafka.listener.ack-on-error:false",
        "spring.kafka.listener.retry-after-error:false",
        "spring.kafka.listener.fail-fast:false"
      })
  public void receiveDUHE(ConsumerRecord<String, GenericRecord> consumerRecord) {

    final GenericRecord value = consumerRecord.value();
    try {
      String messageKey = consumerRecord.key();
      getReference(messageKey).set(EventUtils.avroToPegasusDUHE(value));
      getLatch(messageKey).countDown();
    } catch (IOException e) {
      log.error(
          "Failed to deserialize DUHE message with key: {}, error: {}",
          consumerRecord.key(),
          e.getMessage(),
          e);
      // Continue processing other messages instead of stopping the consumer
    }
  }

  @KafkaListener(
      id = "test-failed-mcp-consumer",
      topics = Topics.FAILED_METADATA_CHANGE_PROPOSAL,
      containerFactory = DEFAULT_EVENT_CONSUMER_NAME,
      properties = {
        "auto.offset.reset:earliest",
        "spring.kafka.listener.ack-mode:manual",
        "spring.kafka.listener.ack-on-error:false",
        "spring.kafka.listener.retry-after-error:false",
        "spring.kafka.listener.fail-fast:false"
      })
  public void receiveFailedMCP(ConsumerRecord<String, GenericRecord> consumerRecord) {

    final GenericRecord value = consumerRecord.value();
    try {
      String messageKey = consumerRecord.key();
      FailedMetadataChangeProposal failedMCP = EventUtils.avroToPegasusFailedMCP(value);
      getReference(messageKey).set(failedMCP.getMetadataChangeProposal());
      getLatch(messageKey).countDown();
    } catch (IOException e) {
      log.error(
          "Failed to deserialize Failed MCP message with key: {}, error: {}",
          consumerRecord.key(),
          e.getMessage(),
          e);
      // Continue processing other messages instead of stopping the consumer
    }
  }
}
