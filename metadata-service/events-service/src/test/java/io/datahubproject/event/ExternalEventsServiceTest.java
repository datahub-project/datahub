package io.datahubproject.event;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.datahubproject.event.exception.UnsupportedTopicException;
import io.datahubproject.event.kafka.KafkaConsumerPool;
import io.datahubproject.event.models.v1.ExternalEvents;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  @BeforeMethod
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(consumerPool.borrowConsumer()).thenReturn(kafkaConsumer);
    topicNames.put(ExternalEventsService.PLATFORM_EVENT_TOPIC_NAME, "CustomerSpecificTopicName");
    service = new ExternalEventsService(consumerPool, objectMapper, topicNames, 10, 100);

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

  @Test(expectedExceptions = UnsupportedTopicException.class)
  public void testPollInvalidTopic() throws Exception {
    service.poll("InvalidTopic", null, 10, 5, null);
  }

  @Test
  public void testShutdown() {
    // Execute
    service.shutdown();

    // Validate
    verify(consumerPool).shutdownPool();
  }
}
