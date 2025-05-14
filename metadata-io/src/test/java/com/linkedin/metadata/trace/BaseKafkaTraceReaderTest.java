package com.linkedin.metadata.trace;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
import java.util.Arrays;
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

  @Test
  public void testGetAllPartitionOffsets_WithCache() {
    // Arrange
    Node mockNode = new Node(0, "localhost", 9092);

    // Setup multiple partitions for testing
    TopicPartitionInfo partitionInfo0 =
        new TopicPartitionInfo(
            0, mockNode, Collections.singletonList(mockNode), Collections.singletonList(mockNode));
    TopicPartitionInfo partitionInfo1 =
        new TopicPartitionInfo(
            1, mockNode, Collections.singletonList(mockNode), Collections.singletonList(mockNode));

    List<TopicPartitionInfo> partitionInfos = Arrays.asList(partitionInfo0, partitionInfo1);
    TopicDescription topicDescription = new TopicDescription(TOPIC_NAME, false, partitionInfos);

    DescribeTopicsResult mockDescribeTopicsResult = mock(DescribeTopicsResult.class);
    when(mockDescribeTopicsResult.all())
        .thenReturn(
            KafkaFuture.completedFuture(Collections.singletonMap(TOPIC_NAME, topicDescription)));
    when(adminClient.describeTopics(anyCollection())).thenReturn(mockDescribeTopicsResult);

    // Setup consumer group offsets for multiple partitions
    TopicPartition topicPartition0 = new TopicPartition(TOPIC_NAME, 0);
    TopicPartition topicPartition1 = new TopicPartition(TOPIC_NAME, 1);

    Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
    offsetMap.put(topicPartition0, new OffsetAndMetadata(100L));
    offsetMap.put(topicPartition1, new OffsetAndMetadata(200L));

    ListConsumerGroupOffsetsResult mockOffsetResult = mock(ListConsumerGroupOffsetsResult.class);
    when(adminClient.listConsumerGroupOffsets(CONSUMER_GROUP)).thenReturn(mockOffsetResult);
    when(mockOffsetResult.partitionsToOffsetAndMetadata())
        .thenReturn(KafkaFuture.completedFuture(offsetMap));

    // Act
    Map<TopicPartition, OffsetAndMetadata> result1 = traceReader.getAllPartitionOffsets(false);

    // Assert
    assertEquals(result1.size(), 2);
    assertEquals(result1.get(topicPartition0).offset(), 100L);
    assertEquals(result1.get(topicPartition1).offset(), 200L);

    // Act again - this should use cache
    Map<TopicPartition, OffsetAndMetadata> result2 = traceReader.getAllPartitionOffsets(false);

    // Assert again
    assertEquals(result2.size(), 2);
    assertEquals(result2.get(topicPartition0).offset(), 100L);
    assertEquals(result2.get(topicPartition1).offset(), 200L);

    // The implementation actually calls describeTopics for each getAllPartitionOffsets call
    // This is because the topicPartitionCache in KafkaTraceReader doesn't cache the topic
    // description
    verify(adminClient, times(2)).describeTopics(anyCollection());
  }

  @Test
  public void testGetAllPartitionOffsets_SkipCache() {
    // Arrange
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

    TopicPartition topicPartition = new TopicPartition(TOPIC_NAME, 0);

    // First call returns offset 100
    ListConsumerGroupOffsetsResult mockOffsetResult1 = mock(ListConsumerGroupOffsetsResult.class);
    when(adminClient.listConsumerGroupOffsets(CONSUMER_GROUP))
        .thenReturn(mockOffsetResult1)
        .thenReturn(mockOffsetResult1); // Return same mock for second call

    Map<TopicPartition, OffsetAndMetadata> offsetMap1 = new HashMap<>();
    offsetMap1.put(topicPartition, new OffsetAndMetadata(100L));

    when(mockOffsetResult1.partitionsToOffsetAndMetadata())
        .thenReturn(KafkaFuture.completedFuture(offsetMap1));

    // Act - first call should populate cache
    Map<TopicPartition, OffsetAndMetadata> result1 = traceReader.getAllPartitionOffsets(false);

    // Assert first result
    assertEquals(result1.size(), 1);
    assertEquals(result1.get(topicPartition).offset(), 100L);

    // Change the mock to return a different offset for the next call
    ListConsumerGroupOffsetsResult mockOffsetResult2 = mock(ListConsumerGroupOffsetsResult.class);
    when(adminClient.listConsumerGroupOffsets(CONSUMER_GROUP)).thenReturn(mockOffsetResult2);

    Map<TopicPartition, OffsetAndMetadata> offsetMap2 = new HashMap<>();
    offsetMap2.put(topicPartition, new OffsetAndMetadata(200L));

    when(mockOffsetResult2.partitionsToOffsetAndMetadata())
        .thenReturn(KafkaFuture.completedFuture(offsetMap2));

    // Act - second call with skipCache=true should bypass cache
    Map<TopicPartition, OffsetAndMetadata> result2 = traceReader.getAllPartitionOffsets(true);

    // Assert second result
    assertEquals(result2.size(), 1);
    assertEquals(result2.get(topicPartition).offset(), 200L);

    // Verify that listConsumerGroupOffsets was called twice
    verify(adminClient, times(2)).listConsumerGroupOffsets(CONSUMER_GROUP);
  }

  @Test
  public void testGetEndOffsets_WithCache() {
    // Arrange
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

    // Setup consumer to return end offsets
    TopicPartition topicPartition = new TopicPartition(TOPIC_NAME, 0);
    Map<TopicPartition, Long> endOffsets = Collections.singletonMap(topicPartition, 500L);
    when(consumer.endOffsets(anyCollection())).thenReturn(endOffsets);

    // Act
    Map<TopicPartition, Long> result1 = traceReader.getEndOffsets(false);

    // Assert
    assertEquals(result1.size(), 1);
    assertEquals(result1.get(topicPartition).longValue(), 500L);

    // Act again - this should use cache
    Map<TopicPartition, Long> result2 = traceReader.getEndOffsets(false);

    // Assert again
    assertEquals(result2.size(), 1);
    assertEquals(result2.get(topicPartition).longValue(), 500L);

    // Verify that endOffsets was called only once
    verify(consumer, times(1)).endOffsets(anyCollection());
  }

  @Test
  public void testGetEndOffsets_SkipCache() {
    // Arrange
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

    // Setup consumer to return end offsets
    TopicPartition topicPartition = new TopicPartition(TOPIC_NAME, 0);
    Map<TopicPartition, Long> endOffsets1 = Collections.singletonMap(topicPartition, 500L);
    Map<TopicPartition, Long> endOffsets2 = Collections.singletonMap(topicPartition, 600L);

    when(consumer.endOffsets(anyCollection())).thenReturn(endOffsets1).thenReturn(endOffsets2);

    // Act
    Map<TopicPartition, Long> result1 = traceReader.getEndOffsets(false);

    // Assert
    assertEquals(result1.size(), 1);
    assertEquals(result1.get(topicPartition).longValue(), 500L);

    // Act again with skipCache=true
    Map<TopicPartition, Long> result2 = traceReader.getEndOffsets(true);

    // Assert again
    assertEquals(result2.size(), 1);
    assertEquals(result2.get(topicPartition).longValue(), 600L);

    // Verify that endOffsets was called twice
    verify(consumer, times(2)).endOffsets(anyCollection());
  }

  @Test
  public void testGetEndOffsets_SpecificPartitions() {
    // Arrange
    Node mockNode = new Node(0, "localhost", 9092);
    TopicPartition topicPartition = new TopicPartition(TOPIC_NAME, 0);
    TopicPartition topicPartition2 = new TopicPartition(TOPIC_NAME, 1);
    List<TopicPartition> partitions = Arrays.asList(topicPartition, topicPartition2);

    // Setup consumer to return end offsets
    Map<TopicPartition, Long> endOffsets = new HashMap<>();
    endOffsets.put(topicPartition, 500L);
    endOffsets.put(topicPartition2, 600L);

    when(consumer.endOffsets(anyCollection())).thenReturn(endOffsets);

    // Act
    Map<TopicPartition, Long> result = traceReader.getEndOffsets(partitions, false);

    // Assert
    assertEquals(result.size(), 2);
    assertEquals(result.get(topicPartition).longValue(), 500L);
    assertEquals(result.get(topicPartition2).longValue(), 600L);

    // Verify that endOffsets was called once
    verify(consumer, times(1)).endOffsets(anyCollection());

    // Verify that assign was called with the correct partitions
    verify(consumer, times(1)).assign(partitions);
  }

  @Test
  public void testGetEndOffsets_SpecificPartitions_WithCache() {
    // Arrange
    TopicPartition topicPartition = new TopicPartition(TOPIC_NAME, 0);
    TopicPartition topicPartition2 = new TopicPartition(TOPIC_NAME, 1);
    List<TopicPartition> partitions = Arrays.asList(topicPartition, topicPartition2);

    // Setup consumer to return end offsets
    Map<TopicPartition, Long> endOffsets = new HashMap<>();
    endOffsets.put(topicPartition, 500L);
    endOffsets.put(topicPartition2, 600L);

    when(consumer.endOffsets(anyCollection())).thenReturn(endOffsets);

    // Act - first call to populate cache
    Map<TopicPartition, Long> result1 = traceReader.getEndOffsets(partitions, false);

    // Assert first result
    assertEquals(result1.size(), 2);
    assertEquals(result1.get(topicPartition).longValue(), 500L);
    assertEquals(result1.get(topicPartition2).longValue(), 600L);

    // Act - second call should use cache
    Map<TopicPartition, Long> result2 = traceReader.getEndOffsets(partitions, false);

    // Assert second result
    assertEquals(result2.size(), 2);
    assertEquals(result2.get(topicPartition).longValue(), 500L);
    assertEquals(result2.get(topicPartition2).longValue(), 600L);

    // Verify that endOffsets was called only once
    verify(consumer, times(1)).endOffsets(anyCollection());
  }

  @Test
  public void testGetEndOffsets_SpecificPartitions_SkipCache() {
    // Arrange
    TopicPartition topicPartition = new TopicPartition(TOPIC_NAME, 0);
    TopicPartition topicPartition2 = new TopicPartition(TOPIC_NAME, 1);
    List<TopicPartition> partitions = Arrays.asList(topicPartition, topicPartition2);

    // Setup consumer to return different end offsets on each call
    Map<TopicPartition, Long> endOffsets1 = new HashMap<>();
    endOffsets1.put(topicPartition, 500L);
    endOffsets1.put(topicPartition2, 600L);

    Map<TopicPartition, Long> endOffsets2 = new HashMap<>();
    endOffsets2.put(topicPartition, 700L);
    endOffsets2.put(topicPartition2, 800L);

    when(consumer.endOffsets(anyCollection())).thenReturn(endOffsets1).thenReturn(endOffsets2);

    // Act - first call to populate cache
    Map<TopicPartition, Long> result1 = traceReader.getEndOffsets(partitions, false);

    // Assert first result
    assertEquals(result1.size(), 2);
    assertEquals(result1.get(topicPartition).longValue(), 500L);
    assertEquals(result1.get(topicPartition2).longValue(), 600L);

    // Act - second call with skipCache=true should bypass cache
    Map<TopicPartition, Long> result2 = traceReader.getEndOffsets(partitions, true);

    // Assert second result
    assertEquals(result2.size(), 2);
    assertEquals(result2.get(topicPartition).longValue(), 700L);
    assertEquals(result2.get(topicPartition2).longValue(), 800L);

    // Verify that endOffsets was called twice
    verify(consumer, times(2)).endOffsets(anyCollection());
  }
}
