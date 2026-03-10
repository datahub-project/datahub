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
    topicNames.put(ExternalEventsService.PLATFORM_EVENT_TOPIC_NAME, "CustomerSpecificTopicName");
    topicNames.put(
        ExternalEventsService.METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME,
        "CustomerSpecificVersionedTopicName");
    topicNames.put(
        ExternalEventsService.METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME,
        "CustomerSpecificTimeseriesTopicName");
    TopicAllowList allowList =
        new TopicAllowList(
            "PlatformEvent_v1,MetadataChangeLog_Versioned_v1,MetadataChangeLog_Timeseries_v1");
    service =
        new ExternalEventsService(
            allowList, consumerPool, objectMapper, topicNames, false, null, 10, 100);

    // Setup to simulate fetching records from Kafka
    String topicName = "CustomerSpecificTopicName";
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

  @Test
  public void testKnownTopicWithPrefixEnabledUsesConventionMapping() throws Exception {
    TopicAllowList allowList =
        new TopicAllowList(
            "PlatformEvent_v1,MetadataChangeLog_Versioned_v1,MetadataChangeLog_Timeseries_v1");
    ExternalEventsService prefixedService =
        new ExternalEventsService(
            allowList, consumerPool, objectMapper, topicNames, true, "acme-", 10, 100);

    when(kafkaConsumer.partitionsFor("InstanceSpecificTopicName"))
        .thenReturn(Collections.emptyList());
    when(objectMapper.writeValueAsString(any())).thenReturn("encodedString");

    prefixedService.poll(ExternalEventsService.PLATFORM_EVENT_TOPIC_NAME, null, 10, 5, null);

    verify(kafkaConsumer).partitionsFor("InstanceSpecificTopicName");
  }

  @Test
  public void testCustomTopicWithPrefixEnabledGetsPrepended() throws Exception {
    TopicAllowList allowList =
        new TopicAllowList(
            "PlatformEvent_v1,MetadataChangeLog_Versioned_v1,MetadataChangeLog_Timeseries_v1,myTopic");
    ExternalEventsService prefixedService =
        new ExternalEventsService(
            allowList, consumerPool, objectMapper, topicNames, true, "acme-", 10, 100);

    when(kafkaConsumer.partitionsFor("acme-myTopic")).thenReturn(Collections.emptyList());
    when(objectMapper.writeValueAsString(any())).thenReturn("encodedString");

    prefixedService.poll("myTopic", null, 10, 5, null);

    verify(kafkaConsumer).partitionsFor("acme-myTopic");
  }

  @Test
  public void testCustomTopicAlreadyPrefixedUsedAsIs() throws Exception {
    TopicAllowList allowList =
        new TopicAllowList(
            "PlatformEvent_v1,MetadataChangeLog_Versioned_v1,MetadataChangeLog_Timeseries_v1,acme-myTopic");
    ExternalEventsService prefixedService =
        new ExternalEventsService(
            allowList, consumerPool, objectMapper, topicNames, true, "acme-", 10, 100);

    when(kafkaConsumer.partitionsFor("acme-myTopic")).thenReturn(Collections.emptyList());
    when(objectMapper.writeValueAsString(any())).thenReturn("encodedString");

    prefixedService.poll("acme-myTopic", null, 10, 5, null);

    verify(kafkaConsumer).partitionsFor("acme-myTopic");
  }

  @Test(expectedExceptions = UnsupportedTopicException.class)
  public void testPrefixEnabledWithoutPrefixBlocksCustomTopics() throws Exception {
    TopicAllowList allowList =
        new TopicAllowList(
            "PlatformEvent_v1,MetadataChangeLog_Versioned_v1,MetadataChangeLog_Timeseries_v1,customTopic");
    ExternalEventsService svc =
        new ExternalEventsService(
            allowList, consumerPool, objectMapper, topicNames, true, "", 10, 100);

    svc.poll("customTopic", null, 10, 5, null);
  }

  @Test
  public void testPrefixDisabledCustomTopicPassesThrough() throws Exception {
    TopicAllowList allowList =
        new TopicAllowList(
            "PlatformEvent_v1,MetadataChangeLog_Versioned_v1,MetadataChangeLog_Timeseries_v1,customTopic");
    ExternalEventsService noPrefixService =
        new ExternalEventsService(
            allowList, consumerPool, objectMapper, topicNames, false, null, 10, 100);

    when(kafkaConsumer.partitionsFor("customTopic")).thenReturn(Collections.emptyList());
    when(objectMapper.writeValueAsString(any())).thenReturn("encodedString");

    noPrefixService.poll("customTopic", null, 10, 5, null);

    verify(kafkaConsumer).partitionsFor("customTopic");
  }
}
