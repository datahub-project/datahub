package com.linkedin.metadata.kafka.listener;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BatchKafkaListenerEndpointTest {

  @Mock private MessageListenerContainer mockContainer;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testSetupListenerContainerSetsMessageListenerDirectly() {
    List<ConsumerRecord<String, GenericRecord>> received = new ArrayList<>();
    BatchMessageListener<String, GenericRecord> listener = received::addAll;

    BatchKafkaListenerEndpoint<String, GenericRecord> endpoint =
        new BatchKafkaListenerEndpoint<>("test-id", "test-group", List.of("topic1"), listener);

    endpoint.setupListenerContainer(mockContainer, null);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<BatchMessageListener<String, GenericRecord>> captor =
        ArgumentCaptor.forClass(BatchMessageListener.class);
    verify(mockContainer).setupMessageListener(captor.capture());
    assertSame(captor.getValue(), listener);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSetupListenerContainerPreservesConsumerRecordWrappers() {
    List<ConsumerRecord<String, GenericRecord>> received = new ArrayList<>();
    BatchMessageListener<String, GenericRecord> listener = received::addAll;

    BatchKafkaListenerEndpoint<String, GenericRecord> endpoint =
        new BatchKafkaListenerEndpoint<>("test-id", "test-group", List.of("topic1"), listener);

    endpoint.setupListenerContainer(mockContainer, null);

    ArgumentCaptor<BatchMessageListener<String, GenericRecord>> captor =
        ArgumentCaptor.forClass(BatchMessageListener.class);
    verify(mockContainer).setupMessageListener(captor.capture());

    ConsumerRecord<String, GenericRecord> record1 = mock(ConsumerRecord.class);
    ConsumerRecord<String, GenericRecord> record2 = mock(ConsumerRecord.class);
    when(record1.topic()).thenReturn("topic1");
    when(record1.timestamp()).thenReturn(1000L);
    when(record2.topic()).thenReturn("topic1");
    when(record2.timestamp()).thenReturn(2000L);

    captor.getValue().onMessage(List.of(record1, record2));

    assertEquals(received.size(), 2);
    assertSame(received.get(0), record1);
    assertSame(received.get(1), record2);
    assertEquals(received.get(0).timestamp(), 1000L);
    assertEquals(received.get(1).timestamp(), 2000L);
  }

  @Test
  public void testEndpointProperties() {
    BatchMessageListener<String, GenericRecord> listener = records -> {};

    BatchKafkaListenerEndpoint<String, GenericRecord> endpoint =
        new BatchKafkaListenerEndpoint<>(
            "my-id", "my-group", List.of("topic-a", "topic-b"), listener);

    assertEquals(endpoint.getId(), "my-id");
    assertEquals(endpoint.getGroupId(), "my-group");
    assertTrue(endpoint.getTopics().containsAll(List.of("topic-a", "topic-b")));
    assertTrue(endpoint.getBatchListener());
    assertFalse(endpoint.getAutoStartup());
    assertFalse(endpoint.isSplitIterables());
    assertNull(endpoint.getGroup());
    assertNull(endpoint.getTopicPartitionsToAssign());
    assertNull(endpoint.getTopicPattern());
    assertNull(endpoint.getClientIdPrefix());
    assertNull(endpoint.getConcurrency());
    assertNull(endpoint.getContainerPostProcessor());
    assertNotNull(endpoint.getConsumerProperties());
    assertTrue(endpoint.getConsumerProperties().isEmpty());
  }
}
