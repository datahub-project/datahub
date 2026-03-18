package io.datahubproject.event;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.datahubproject.event.exception.UnsupportedTopicException;
import io.datahubproject.event.kafka.CheckedConsumer;
import io.datahubproject.event.kafka.KafkaConsumerPool;
import io.datahubproject.event.models.v1.ExternalEvents;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ExternalEventsServiceTest {
  @Mock private KafkaConsumerPool consumerPool;
  @Mock private KafkaConsumer<String, GenericRecord> kafkaConsumer;
  @Mock private ObjectMapper objectMapper;
  private ExternalEventsService service;
  private Map<String, String> topicNames = new HashMap<>();
  private CheckedConsumer checkedConsumer;

  @BeforeMethod
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    checkedConsumer =
        new CheckedConsumer(kafkaConsumer, Duration.ofSeconds(2), Duration.ofMinutes(5), null);
    when(consumerPool.borrowConsumer(anyLong(), any(TimeUnit.class), anyString()))
        .thenReturn(checkedConsumer);
    topicNames.put(ExternalEventsService.PLATFORM_EVENT_TOPIC_NAME, "InstanceSpecificTopicName");
    topicNames.put(
        ExternalEventsService.METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME,
        "InstanceSpecificVersionedTopicName");
    topicNames.put(
        ExternalEventsService.METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME,
        "InstanceSpecificTimeseriesTopicName");
    Set<String> allowedTopics =
        Set.of(
            "PlatformEvent_v1",
            "MetadataChangeLog_Versioned_v1",
            "MetadataChangeLog_Timeseries_v1");
    service =
        new ExternalEventsService(allowedTopics, consumerPool, objectMapper, topicNames, 10, 100);

    // Setup to simulate fetching records from Kafka
    String topicName = "InstanceSpecificTopicName";
    int partition = 0;
    TopicPartition topicPartition = new TopicPartition(topicName, partition);

    String userSchemaJson =
        "{"
            + "\"type\": \"record\","
            + "\"name\": \"User\","
            + "\"fields\": ["
            + "  {\"name\": \"id\", \"type\": \"int\"},"
            + "  {\"name\": \"name\", \"type\": \"string\"},"
            + "  {\"name\": \"email\", \"type\": \"string\"}"
            + "]}";

    Schema userSchema = new Schema.Parser().parse(userSchemaJson);

    GenericRecord testEvent1 = new GenericData.Record(userSchema);
    testEvent1.put("id", 1);
    testEvent1.put("name", "John Doe");
    testEvent1.put("email", "john.doe@example.com");

    GenericRecord testEvent2 = new GenericData.Record(userSchema);
    testEvent2.put("id", 2);
    testEvent2.put("name", "Jane Doe");
    testEvent2.put("email", "jane.doe@example.com");

    // Create a list of ConsumerRecords to return
    List<ConsumerRecord<String, GenericRecord>> recordsList =
        Arrays.asList(
            new ConsumerRecord<>(topicName, partition, 0, "key1", testEvent1),
            new ConsumerRecord<>(topicName, partition, 1, "key2", testEvent2));

    // Create a Map<TopicPartition, List<ConsumerRecord<String, GenericRecord>>> for the
    // ConsumerRecords constructor
    Map<TopicPartition, List<ConsumerRecord<String, GenericRecord>>> recordsMap = new HashMap<>();
    recordsMap.put(topicPartition, recordsList);

    // Create the ConsumerRecords instance
    ConsumerRecords<String, GenericRecord> consumerRecords = new ConsumerRecords<>(recordsMap);

    // When the consumer polls, return this ConsumerRecords instance
    when(kafkaConsumer.poll(eq(Duration.ofMillis(1000)))).thenReturn(consumerRecords);
  }

  @Test
  public void testPollValidTopic() throws Exception {
    // Mocking Kafka and ObjectMapper behaviors
    when(kafkaConsumer.partitionsFor(anyString())).thenReturn(Collections.emptyList());
    when(objectMapper.writeValueAsString(any())).thenReturn("encodedString");

    // Execute
    ExternalEvents events =
        service.poll(ExternalEventsService.PLATFORM_EVENT_TOPIC_NAME, null, 10, 5, null);

    // Validate
    assertNotNull(events);
    verify(kafkaConsumer, atLeastOnce()).poll(any());
  }

  @Test
  public void testPollValidMetadataChangeLogVersionedTopic() throws Exception {
    // Mocking Kafka and ObjectMapper behaviors
    when(kafkaConsumer.partitionsFor(anyString())).thenReturn(Collections.emptyList());
    when(objectMapper.writeValueAsString(any())).thenReturn("encodedString");

    // Execute
    ExternalEvents events =
        service.poll(
            ExternalEventsService.METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME, null, 10, 5, null);

    // Validate
    assertNotNull(events);
    verify(kafkaConsumer, atLeastOnce()).poll(any());
  }

  @Test
  public void testPollValidMetadataChangeLogTimeseriesTopic() throws Exception {
    // Mocking Kafka and ObjectMapper behaviors
    when(kafkaConsumer.partitionsFor(anyString())).thenReturn(Collections.emptyList());
    when(objectMapper.writeValueAsString(any())).thenReturn("encodedString");

    // Execute
    ExternalEvents events =
        service.poll(
            ExternalEventsService.METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME, null, 10, 5, null);

    // Validate
    assertNotNull(events);
    verify(kafkaConsumer, atLeastOnce()).poll(any());
  }

  @Test
  public void testShutdown() {
    // Execute
    service.shutdown();

    // Validate
    verify(consumerPool).shutdownPool();
  }

  @Test(expectedExceptions = UnsupportedTopicException.class)
  public void testPollInvalidTopic() throws Exception {
    service.poll("SomeUnknownTopic", null, 10, 5, null);
  }

  @Test
  public void testKnownTopicUsesMapping() throws Exception {
    when(kafkaConsumer.partitionsFor("InstanceSpecificTopicName"))
        .thenReturn(Collections.emptyList());
    when(objectMapper.writeValueAsString(any())).thenReturn("encodedString");

    service.poll(ExternalEventsService.PLATFORM_EVENT_TOPIC_NAME, null, 10, 5, null);

    verify(kafkaConsumer).partitionsFor("InstanceSpecificTopicName");
  }

  @Test
  public void testCustomTopicWithNonDefaultName() throws Exception {
    Map<String, String> names = new HashMap<>(topicNames);
    names.put("myTopic", "acme_myTopic");
    Set<String> allowedTopics = Set.of("PlatformEvent_v1", "myTopic");
    ExternalEventsService svc =
        new ExternalEventsService(allowedTopics, consumerPool, objectMapper, names, 10, 100);

    when(kafkaConsumer.partitionsFor("acme_myTopic")).thenReturn(Collections.emptyList());
    when(objectMapper.writeValueAsString(any())).thenReturn("encodedString");

    svc.poll("myTopic", null, 10, 5, null);

    verify(kafkaConsumer).partitionsFor("acme_myTopic");
  }

  @Test
  public void testWorkEventWithPrefixedKafkaTopicName() throws Exception {
    // Simulates real-world scenario: displayName=WorkEvent_v1, actual Kafka
    // topic=2391478-acme_WorkEvent_v1
    Map<String, String> names = new HashMap<>(topicNames);
    names.put("WorkEvent_v1", "2391478-acme_WorkEvent_v1");
    Set<String> allowedTopics = Set.of("PlatformEvent_v1", "WorkEvent_v1");
    ExternalEventsService svc =
        new ExternalEventsService(allowedTopics, consumerPool, objectMapper, names, 10, 100);

    when(kafkaConsumer.partitionsFor("2391478-acme_WorkEvent_v1"))
        .thenReturn(Collections.emptyList());
    when(objectMapper.writeValueAsString(any())).thenReturn("encodedString");

    // API call uses displayName (WorkEvent_v1)
    svc.poll("WorkEvent_v1", null, 10, 5, null);

    // Kafka consumer should be called with actual topic name (2391478-acme_WorkEvent_v1)
    verify(kafkaConsumer).partitionsFor("2391478-acme_WorkEvent_v1");
  }

  @Test(expectedExceptions = UnsupportedTopicException.class)
  public void testUnmappedTopicThrows() throws Exception {
    Set<String> allowedTopics = Set.of("PlatformEvent_v1", "customTopic");
    ExternalEventsService svc =
        new ExternalEventsService(allowedTopics, consumerPool, objectMapper, topicNames, 10, 100);

    svc.poll("customTopic", null, 10, 5, null);
  }
}
