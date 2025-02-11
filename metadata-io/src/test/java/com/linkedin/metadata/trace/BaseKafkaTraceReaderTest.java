package com.linkedin.metadata.trace;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.TraceContext;
import io.datahubproject.openapi.v1.models.TraceStorageStatus;
import io.datahubproject.openapi.v1.models.TraceWriteStatus;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public abstract class BaseKafkaTraceReaderTest<M extends RecordTemplate> {
  protected static final String TOPIC_NAME = "test-topic";
  protected static final String CONSUMER_GROUP = "test-group";
  protected static final String TRACE_ID = "test-trace-id";
  protected static final String ASPECT_NAME = "status";
  protected static final Urn TEST_URN = UrnUtils.getUrn("urn:li:container:123");

  @Mock protected AdminClient adminClient;
  @Mock protected Consumer<String, GenericRecord> consumer;
  protected ExecutorService executorService;
  protected KafkaTraceReader<M> traceReader;

  abstract KafkaTraceReader<M> buildTraceReader();

  abstract M buildMessage(@Nullable SystemMetadata systemMetadata);

  abstract GenericRecord toGenericRecord(M message) throws IOException;

  abstract M fromGenericRecord(GenericRecord genericRecord) throws IOException;

  @BeforeMethod(alwaysRun = true)
  public void setup() {
    MockitoAnnotations.openMocks(this);
    executorService = Executors.newSingleThreadExecutor();
    traceReader = buildTraceReader();
    setupDefaultMocks();
  }

  protected void setupDefaultMocks() {
    // Mock topic description
    Node mockNode = new Node(0, "localhost", 9092);
    TopicPartitionInfo partitionInfo =
        new TopicPartitionInfo(
            0, mockNode, Collections.singletonList(mockNode), Collections.singletonList(mockNode));
    TopicDescription topicDescription =
        new TopicDescription(TOPIC_NAME, false, Collections.singletonList(partitionInfo));

    DescribeTopicsResult mockDescribeTopicsResult = mock(DescribeTopicsResult.class);
    when(mockDescribeTopicsResult.all())
        .thenReturn(
            KafkaFuture.completedFuture(Collections.singletonMap(TOPIC_NAME, topicDescription)));
    when(adminClient.describeTopics(anyCollection())).thenReturn(mockDescribeTopicsResult);

    // Mock consumer group offset lookup
    ListConsumerGroupOffsetsResult mockOffsetResult = mock(ListConsumerGroupOffsetsResult.class);
    when(adminClient.listConsumerGroupOffsets(CONSUMER_GROUP)).thenReturn(mockOffsetResult);
    when(mockOffsetResult.partitionsToOffsetAndMetadata())
        .thenReturn(
            KafkaFuture.completedFuture(
                Collections.singletonMap(
                    new TopicPartition(TOPIC_NAME, 0), new OffsetAndMetadata(100L))));

    // Mock consumer behavior
    when(consumer.poll(any(Duration.class))).thenReturn(mock(ConsumerRecords.class));
  }

  @Test
  public void testRead_WithValidGenericRecord() throws Exception {
    // Arrange
    M expectedMessage = buildMessage(null);
    GenericRecord genericRecord = toGenericRecord(expectedMessage);

    // Act
    Optional<M> result = traceReader.read(genericRecord);

    // Assert
    assertTrue(result.isPresent());
    assertEquals(result.get(), expectedMessage);
  }

  @Test
  public void testRead_WithNullGenericRecord() {
    Optional<M> result = traceReader.read(null);
    assertFalse(result.isPresent());
  }

  @Test
  public void testMatchConsumerRecord_WithMatchingTraceAndAspect() throws IOException {
    // Arrange
    ConsumerRecord<String, GenericRecord> mockConsumerRecord = mock(ConsumerRecord.class);

    SystemMetadata systemMetadata = new SystemMetadata();
    Map<String, String> properties = new HashMap<>();
    properties.put(TraceContext.TELEMETRY_TRACE_KEY, TRACE_ID);
    systemMetadata.setProperties(new StringMap(properties));

    GenericRecord genericRecord = toGenericRecord(buildMessage(systemMetadata));
    when(mockConsumerRecord.value()).thenReturn(genericRecord);

    // Act
    Optional<Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>> result =
        traceReader.matchConsumerRecord(mockConsumerRecord, TRACE_ID, ASPECT_NAME);

    // Assert
    assertTrue(result.isPresent());
    assertEquals(result.get().getFirst(), mockConsumerRecord);
    assertEquals(result.get().getSecond(), systemMetadata);
  }

  @Test
  public void testTracePendingStatuses() throws IOException {
    // Arrange
    List<String> aspectNames = Collections.singletonList(ASPECT_NAME);
    Map<Urn, List<String>> urnAspectPairs = Collections.singletonMap(TEST_URN, aspectNames);
    long timestamp = System.currentTimeMillis();

    // Mock topic partition
    TopicPartition topicPartition = new TopicPartition(TOPIC_NAME, 0);

    // Mock consumer group offset lookup (lower offset)
    OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(100L);
    ListConsumerGroupOffsetsResult mockOffsetResult = mock(ListConsumerGroupOffsetsResult.class);
    when(adminClient.listConsumerGroupOffsets(CONSUMER_GROUP)).thenReturn(mockOffsetResult);
    when(mockOffsetResult.partitionsToOffsetAndMetadata())
        .thenReturn(
            KafkaFuture.completedFuture(
                Collections.singletonMap(topicPartition, offsetAndMetadata)));

    // Mock offset lookup by timestamp
    when(consumer.offsetsForTimes(any()))
        .thenReturn(
            Collections.singletonMap(topicPartition, new OffsetAndTimestamp(150L, timestamp)));

    // Create system metadata with trace ID
    SystemMetadata systemMetadata = new SystemMetadata();
    Map<String, String> properties = new HashMap<>();
    properties.put(TraceContext.TELEMETRY_TRACE_KEY, TRACE_ID);
    systemMetadata.setProperties(new StringMap(properties));

    // Build message with metadata
    M message = buildMessage(systemMetadata);
    GenericRecord genericRecord = toGenericRecord(message);

    // Mock consumer record fetch with higher offset than consumer offset
    ConsumerRecord<String, GenericRecord> mockRecord =
        new ConsumerRecord<>(TOPIC_NAME, 0, 150L, TEST_URN.toString(), genericRecord);
    ConsumerRecords<String, GenericRecord> mockRecords = mock(ConsumerRecords.class);
    when(mockRecords.isEmpty()).thenReturn(false);
    when(mockRecords.records(any(TopicPartition.class)))
        .thenReturn(Collections.singletonList(mockRecord));
    when(consumer.poll(any(Duration.class))).thenReturn(mockRecords);

    // Act
    Map<Urn, Map<String, TraceStorageStatus>> result =
        traceReader.tracePendingStatuses(urnAspectPairs, TRACE_ID, timestamp);

    // Assert
    assertTrue(result.containsKey(TEST_URN));
    assertTrue(result.get(TEST_URN).containsKey(ASPECT_NAME));
    assertEquals(result.get(TEST_URN).get(ASPECT_NAME).getWriteStatus(), TraceWriteStatus.PENDING);
  }

  @Test
  public void testFindMessages() throws Exception {
    // Arrange
    List<String> aspectNames = Collections.singletonList(ASPECT_NAME);
    Map<Urn, List<String>> urnAspectPairs = Collections.singletonMap(TEST_URN, aspectNames);
    long timestamp = System.currentTimeMillis();

    // Mock topic partition assignment and offsets
    TopicPartition topicPartition = new TopicPartition(TOPIC_NAME, 0);
    OffsetAndTimestamp offsetAndTimestamp = new OffsetAndTimestamp(100L, timestamp);
    when(consumer.offsetsForTimes(any()))
        .thenReturn(Collections.singletonMap(topicPartition, offsetAndTimestamp));

    // Mock system metadata
    SystemMetadata systemMetadata = new SystemMetadata();
    Map<String, String> properties = new HashMap<>();
    properties.put(TraceContext.TELEMETRY_TRACE_KEY, TRACE_ID);
    systemMetadata.setProperties(new StringMap(properties));
    M message = buildMessage(systemMetadata);

    // Mock consumer record fetch
    ConsumerRecord<String, GenericRecord> mockRecord =
        new ConsumerRecord<>(TOPIC_NAME, 0, 100L, TEST_URN.toString(), toGenericRecord(message));
    ConsumerRecords<String, GenericRecord> mockRecords = mock(ConsumerRecords.class);
    when(mockRecords.records(any(TopicPartition.class)))
        .thenReturn(Collections.singletonList(mockRecord));
    when(consumer.poll(any(Duration.class))).thenReturn(mockRecords);

    // Act
    Map<Urn, Map<String, Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>>> result =
        traceReader.findMessages(urnAspectPairs, TRACE_ID, timestamp);

    // Assert
    assertTrue(result.containsKey(TEST_URN));
    assertTrue(result.get(TEST_URN).containsKey(ASPECT_NAME));
    assertEquals(result.get(TEST_URN).get(ASPECT_NAME).getFirst(), mockRecord);
    assertEquals(result.get(TEST_URN).get(ASPECT_NAME).getSecond(), systemMetadata);
  }
}
