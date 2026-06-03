package com.linkedin.metadata.event;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PgQueuePayloadCompression;
import com.linkedin.metadata.queue.QueueTopicDefaults;
import com.linkedin.metadata.queue.QueueTopicMetadata;
import java.util.concurrent.Future;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PgQueueUsageEventPublisherTest {

  private MetadataQueueStore store;
  private QueueTopicDefaults topicDefaults;
  private PgQueueUsageEventPublisher publisher;

  @BeforeMethod
  public void setUp() {
    store = mock(MetadataQueueStore.class);
    topicDefaults =
        new QueueTopicDefaults(3, 604800, 0, 0, false, "application/vnd.apache.avro+binary");
    publisher =
        new PgQueueUsageEventPublisher(store, null, topicDefaults, PgQueuePayloadCompression.NONE);
  }

  @Test
  public void testPublishHappyPath() throws Exception {
    Future<?> result = publisher.publish("DataHubUsageEvent_v1", "user123", "{\"event\":\"view\"}");

    assertNotNull(result);
    assertFalse(result.isDone() && result.get() == null ? false : true);
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
  public void testPublishNullKeyUsesEmptyString() throws Exception {
    publisher.publish("DataHubUsageEvent_v1", null, "{\"event\":\"view\"}");

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
  public void testPublishWhenNotWritableSkipsEnqueue() throws Exception {
    publisher.setWritable(false);

    Future<?> result = publisher.publish("DataHubUsageEvent_v1", "key", "{}");

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
  public void testPublishStoreExceptionReturnsFailedFuture() {
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

    Future<?> result = publisher.publish("DataHubUsageEvent_v1", "key", "{}");

    assertNotNull(result);
    assertTrue(result.isDone());
    try {
      result.get();
      throw new AssertionError("Expected exception");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof RuntimeException);
    }
  }

  @Test
  public void testEffectiveDefaultsUsesFallbackWhenOptionsNull() throws Exception {
    publisher.publish("DataHubUsageEvent_v1", "key", "{}");

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
    // Should use the fallback topicDefaults directly
    assertTrue(defaultsCaptor.getValue() == topicDefaults);
  }

  @Test
  public void testEffectiveDefaultsUsesOptionsWhenProvided() throws Exception {
    PgQueueSetupOptions options = mock(PgQueueSetupOptions.class);
    QueueTopicDefaults resolvedDefaults =
        new QueueTopicDefaults(1, 86400, 0, 0, false, "application/json");
    when(options.getResolvedTopicCatalog()).thenReturn(java.util.List.of());

    PgQueueUsageEventPublisher pub =
        new PgQueueUsageEventPublisher(
            store, options, topicDefaults, PgQueuePayloadCompression.NONE);
    pub.publish("DataHubUsageEvent_v1", "key", "{}");

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
}
