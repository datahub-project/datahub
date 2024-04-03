package io.datahubproject.openapi.test;

import static com.linkedin.metadata.Constants.*;
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
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.PlatformEventHeader;
import com.linkedin.mxe.Topics;
import com.linkedin.platform.event.v1.EntityChangeEvent;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import mock.MockEntitySpec;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.Test;

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
public class SchemaRegistryControllerTest extends AbstractTestNGSpringContextTests {
  private static final String CONFLUENT_PLATFORM_VERSION = "7.2.2";

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
    registry.add("kafka.schemaRegistry.url", () -> "http://localhost:53222/api/");
  }

  @Autowired EventProducer _producer;

  private final CountDownLatch mcpLatch = new CountDownLatch(1);

  private final AtomicReference<MetadataChangeProposal> mcpRef = new AtomicReference<>();

  private final CountDownLatch mclLatch = new CountDownLatch(1);

  private final AtomicReference<MetadataChangeLog> mclRef = new AtomicReference<>();

  private final CountDownLatch peLatch = new CountDownLatch(1);

  private final AtomicReference<PlatformEvent> peRef = new AtomicReference<>();

  @Test
  public void testMCPConsumption()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    final Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)");
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
    final boolean messageConsumed = mcpLatch.await(10, TimeUnit.SECONDS);
    assertTrue(messageConsumed);
    assertEquals(mcpLatch.getCount(), 0);
    assertEquals(mcpRef.get(), gmce);
  }

  @Test
  public void testMCLConsumption()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    final Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)");
    final DatasetProperties datasetProperties = new DatasetProperties();
    datasetProperties.setName("Foo Bar");
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
    final boolean messageConsumed = mclLatch.await(10, TimeUnit.SECONDS);
    assertTrue(messageConsumed);
    assertEquals(mclLatch.getCount(), 0);
    assertEquals(mclRef.get(), metadataChangeLog);
  }

  @Test
  public void testPEConsumption()
      throws InterruptedException, ExecutionException, TimeoutException {

    final Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)");
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
        .producePlatformEvent(CHANGE_EVENT_PLATFORM_EVENT_NAME, "Some key", platformEvent)
        .get(10, TimeUnit.SECONDS);

    final boolean messageConsumed = peLatch.await(10, TimeUnit.SECONDS);
    assertTrue(messageConsumed);
    assertEquals(peLatch.getCount(), 0);
    assertEquals(peRef.get(), platformEvent);
  }

  @KafkaListener(
      id = "test-mcp-consumer",
      topics = Topics.METADATA_CHANGE_PROPOSAL,
      containerFactory = "kafkaEventConsumer",
      properties = {"auto.offset.reset:earliest"})
  public void receiveMCP(ConsumerRecord<String, GenericRecord> consumerRecord) {

    final GenericRecord value = consumerRecord.value();
    try {
      mcpRef.set(EventUtils.avroToPegasusMCP(value));
      mcpLatch.countDown();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @KafkaListener(
      id = "test-mcl-consumer",
      topics = Topics.METADATA_CHANGE_LOG_VERSIONED,
      containerFactory = "kafkaEventConsumer",
      properties = {"auto.offset.reset:earliest"})
  public void receiveMCL(ConsumerRecord<String, GenericRecord> consumerRecord) {

    final GenericRecord value = consumerRecord.value();
    try {
      mclRef.set(EventUtils.avroToPegasusMCL(value));
      mclLatch.countDown();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @KafkaListener(
      id = "test-pe-consumer",
      topics = Topics.PLATFORM_EVENT,
      containerFactory = "kafkaEventConsumer",
      properties = {"auto.offset.reset:earliest"})
  public void receivePE(ConsumerRecord<String, GenericRecord> consumerRecord) {

    final GenericRecord value = consumerRecord.value();
    try {
      peRef.set(EventUtils.avroToPegasusPE(value));
      peLatch.countDown();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
