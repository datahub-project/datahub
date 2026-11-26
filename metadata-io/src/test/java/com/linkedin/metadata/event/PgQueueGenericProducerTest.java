package com.linkedin.metadata.event;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PgQueuePayloadCompression;
import com.linkedin.metadata.queue.QueueTopicDefaults;
import com.linkedin.metadata.queue.QueueTopicMetadata;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PgQueueGenericProducerTest {

  private MetadataQueueStore store;
  private QueueTopicDefaults topicDefaults;
  private PgQueueGenericProducer producer;

  @BeforeMethod
  public void setUp() {
    store = mock(MetadataQueueStore.class);
    topicDefaults =
        new QueueTopicDefaults(3, 604800, 0, 0, false, "application/vnd.apache.avro+binary");
    producer =
        new PgQueueGenericProducer(store, null, topicDefaults, PgQueuePayloadCompression.NONE);
  }

  @Test
  public void testSendHappyPath() throws Exception {
    Future<?> result =
        producer.send(
            new ProducerRecord<>("DataHubUsageEvent_v1", "user123", "{\"event\":\"view\"}"), null);

    assertNotNull(result);
    assertTrue(result.isDone());
    verify(store)
        .enqueue(
            eq("DataHubUsageEvent_v1"),
            eq("user123"),
            any(QueueTopicDefaults.class),
            eq(QueueTopicMetadata.DEFAULT_PRIORITY),
            any(byte[].class),
            any(),
            any(),
            eq(PgQueuePayloadCompression.NONE));
  }

  @Test
  public void testSendNullKeyUsesEmptyString() {
    producer.send(new ProducerRecord<>("DataHubUsageEvent_v1", null, "{}"), null);

    ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
    verify(store)
        .enqueue(
            anyString(),
            keyCaptor.capture(),
            any(QueueTopicDefaults.class),
            anyInt(),
            any(byte[].class),
            any(),
            any(),
            any());
    assertTrue(keyCaptor.getValue().isEmpty());
  }

  @Test
  public void testSendWhenNotWritableSkipsEnqueue() {
    producer.setWritable(false);

    Future<?> result =
        producer.send(new ProducerRecord<>("DataHubUsageEvent_v1", "key", "{}"), null);

    assertNotNull(result);
    verify(store, never())
        .enqueue(
            anyString(),
            anyString(),
            any(QueueTopicDefaults.class),
            anyInt(),
            any(byte[].class),
            any(),
            any(),
            any());
  }

  @Test
  public void testSendInvokesCallbackOnSuccess() {
    AtomicReference<Exception> error = new AtomicReference<>();
    Callback callback = (metadata, exception) -> error.set(exception);

    producer.send(new ProducerRecord<>("DataHubUsageEvent_v1", "key", "{}"), callback);

    assertNull(error.get());
  }

  @Test
  public void testSendStoreExceptionReturnsFailedFutureAndCallback() {
    when(store.enqueue(
            anyString(),
            anyString(),
            any(QueueTopicDefaults.class),
            anyInt(),
            any(byte[].class),
            any(),
            any(),
            any()))
        .thenThrow(new RuntimeException("DB down"));

    AtomicReference<Exception> callbackError = new AtomicReference<>();
    Future<?> result =
        producer.send(
            new ProducerRecord<>("DataHubUsageEvent_v1", "key", "{}"),
            (metadata, exception) -> callbackError.set(exception));

    assertNotNull(result);
    assertTrue(result.isDone());
    assertTrue(callbackError.get() instanceof RuntimeException);
    try {
      result.get();
      throw new AssertionError("Expected exception");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof RuntimeException);
    }
  }

  @Test
  public void testEffectiveDefaultsUsesFallbackWhenOptionsNull() {
    producer.send(new ProducerRecord<>("DataHubUsageEvent_v1", "key", "{}"), null);

    ArgumentCaptor<QueueTopicDefaults> defaultsCaptor =
        ArgumentCaptor.forClass(QueueTopicDefaults.class);
    verify(store)
        .enqueue(
            anyString(),
            anyString(),
            defaultsCaptor.capture(),
            anyInt(),
            any(byte[].class),
            any(),
            any(),
            any());
    assertTrue(defaultsCaptor.getValue() == topicDefaults);
  }

  @Test
  public void testEffectiveDefaultsUsesOptionsWhenProvided() {
    PgQueueSetupOptions options = mock(PgQueueSetupOptions.class);
    when(options.getResolvedTopicCatalog()).thenReturn(java.util.List.of());

    PgQueueGenericProducer withOptions =
        new PgQueueGenericProducer(store, options, topicDefaults, PgQueuePayloadCompression.NONE);
    withOptions.send(new ProducerRecord<>("DataHubUsageEvent_v1", "key", "{}"), null);

    verify(store)
        .enqueue(
            anyString(),
            anyString(),
            any(QueueTopicDefaults.class),
            anyInt(),
            any(byte[].class),
            any(),
            any(),
            any());
  }

  @Test
  public void testFlushIsNoOp() {
    producer.flush();
  }
}
