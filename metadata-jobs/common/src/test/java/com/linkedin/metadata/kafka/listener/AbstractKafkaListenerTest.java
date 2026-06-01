package com.linkedin.metadata.kafka.listener;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.kafka.InboundMetadataEnvelope;
import com.linkedin.metadata.utils.metrics.CascadeOperationContext;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.MDC;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AbstractKafkaListenerTest {

  private static final String CONSUMER_GROUP = "test-group";
  private static final String TOPIC = "test-topic";

  private HookOne hook1;
  private HookTwo hook2;
  @Mock private MetricUtils metricUtils;
  @Mock private OperationContext operationContext;

  private StubListener listener;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    MDC.clear();
    when(metricUtils.getRegistry()).thenReturn(new SimpleMeterRegistry());
    when(operationContext.getMetricUtils()).thenReturn(Optional.of(metricUtils));
    doAnswer(
            inv -> {
              inv.getArgument(3, Runnable.class).run();
              return null;
            })
        .when(operationContext)
        .withQueueSpan(
            anyString(),
            org.mockito.ArgumentMatchers.<SystemMetadata>any(),
            anyString(),
            any(Runnable.class),
            any(String[].class));
    doAnswer(
            inv -> {
              inv.getArgument(3, Runnable.class).run();
              return null;
            })
        .when(operationContext)
        .withQueueSpan(
            anyString(),
            org.mockito.ArgumentMatchers.<List<SystemMetadata>>any(),
            anyString(),
            any(Runnable.class),
            any(String[].class));
    doAnswer(
            inv -> {
              Runnable op = inv.getArgument(1);
              op.run();
              return null;
            })
        .when(operationContext)
        .withSpan(anyString(), any(Runnable.class), any(String[].class));

    hook1 = new HookOne();
    hook2 = new HookTwo();
    listener = new StubListener();
    listener.init(
        operationContext, CONSUMER_GROUP, List.of(hook1, hook2), false, Collections.emptyMap());
  }

  @AfterMethod
  public void tearDown() {
    MDC.clear();
  }

  @Test
  public void initInitializesHooks() {
    assertEquals(listener.getConsumerGroupId(), CONSUMER_GROUP);
    assertEquals(listener.getHooks().size(), 2);
  }

  @Test
  public void consumeEnvelopeInvokesHooks() throws Exception {
    TestEvent event = new TestEvent("evt-1");
    listener.nextEvent = event;

    InboundMetadataEnvelope<String> envelope =
        InboundMetadataEnvelope.<String>builder()
            .messagingSystem(MetricUtils.MESSAGING_SYSTEM_KAFKA)
            .logicalTopic(TOPIC)
            .key("key")
            .payload("payload")
            .enqueuedAtMillis(System.currentTimeMillis())
            .consumerGroupId(CONSUMER_GROUP)
            .kafkaPartition(0)
            .kafkaOffset(1L)
            .serializedValueSize(10)
            .build();

    listener.consumeEnvelope(envelope);

    assertTrue(hook1.invoked);
    assertTrue(hook2.invoked);
    verify(metricUtils)
        .increment(eq(StubListener.class), eq(CONSUMER_GROUP + "_consumed_event_count"), eq(1d));
  }

  @Test
  public void consumeDelegatesToProcessEnvelope() throws Exception {
    TestEvent event = new TestEvent("evt-kafka");
    listener.nextEvent = event;

    ConsumerRecord<String, String> record = new ConsumerRecord<>(TOPIC, 0, 5L, "key", "payload");
    record.headers();

    listener.consume(record);

    assertTrue(hook1.invoked);
  }

  @Test
  public void conversionFailureIncrementsMetricAndSkipsHooks() throws Exception {
    listener.convertThrows = true;

    listener.consumeEnvelope(
        InboundMetadataEnvelope.<String>builder()
            .messagingSystem(MetricUtils.MESSAGING_SYSTEM_KAFKA)
            .logicalTopic(TOPIC)
            .payload("bad")
            .enqueuedAtMillis(0L)
            .build());

    verify(metricUtils)
        .increment(eq(StubListener.class), eq(CONSUMER_GROUP + "_conversion_failure"), eq(1d));
    assertFalse(hook1.invoked);
  }

  @Test
  public void shouldSkipProcessingSkipsHooks() throws Exception {
    listener.skipNext = true;
    listener.nextEvent = new TestEvent("skip");

    listener.consumeEnvelope(
        InboundMetadataEnvelope.<String>builder()
            .messagingSystem(MetricUtils.MESSAGING_SYSTEM_KAFKA)
            .logicalTopic(TOPIC)
            .payload("payload")
            .enqueuedAtMillis(0L)
            .build());

    assertFalse(hook1.invoked);
    verify(metricUtils, never())
        .increment(eq(StubListener.class), eq(CONSUMER_GROUP + "_consumed_event_count"), anyInt());
  }

  @Test
  public void hookFailureContinuesWithRemainingHooks() throws Exception {
    TestEvent event = new TestEvent("evt-hook-fail");
    listener.nextEvent = event;
    hook1.failOnInvoke = true;

    listener.consumeEnvelope(
        InboundMetadataEnvelope.<String>builder()
            .messagingSystem(MetricUtils.MESSAGING_SYSTEM_KAFKA)
            .logicalTopic(TOPIC)
            .payload("payload")
            .enqueuedAtMillis(0L)
            .build());

    assertTrue(hook1.invoked);
    assertTrue(hook2.invoked);
    verify(metricUtils).increment(eq(StubListener.class), eq("HookOne_failure"), eq(1d));
  }

  @Test
  public void cascadeOperationIdPropagatesToMdc() throws Exception {
    TestEvent event = new TestEvent("evt-cascade");
    StringMap props = new StringMap();
    props.put(CascadeOperationContext.SYSTEM_METADATA_CASCADE_ID_KEY, "cascade-123");
    event.systemMetadata = new SystemMetadata().setProperties(props);
    listener.nextEvent = event;

    listener.consumeEnvelope(
        InboundMetadataEnvelope.<String>builder()
            .messagingSystem(MetricUtils.MESSAGING_SYSTEM_KAFKA)
            .logicalTopic(TOPIC)
            .payload("payload")
            .enqueuedAtMillis(0L)
            .build());

    assertEquals(hook1.cascadeIdSeenDuringInvoke, "cascade-123");
    assertNull(MDC.get(CascadeOperationContext.MDC_CASCADE_OPERATION_ID));
  }

  /** Minimal listener stub for exercising {@link AbstractKafkaListener} in this module. */
  static final class StubListener
      extends AbstractKafkaListener<TestEvent, EventHook<TestEvent>, String> {

    TestEvent nextEvent = new TestEvent("default");
    boolean skipNext;
    boolean convertThrows;

    @Override
    public void consumeBatch(List<ConsumerRecord<String, String>> consumerRecords) {
      for (ConsumerRecord<String, String> record : consumerRecords) {
        consume(record);
      }
    }

    @Override
    public TestEvent convertRecord(String record) throws IOException {
      if (convertThrows) {
        throw new IOException("conversion failed");
      }
      return nextEvent;
    }

    @Override
    protected void setMDCContext(TestEvent event) {
      MDC.put("testEventId", event.id);
    }

    @Override
    protected boolean shouldSkipProcessing(TestEvent event) {
      return skipNext;
    }

    @Override
    protected List<String> getFineGrainedLoggingAttributes(TestEvent event) {
      return List.of();
    }

    @Override
    protected SystemMetadata getSystemMetadata(TestEvent event) {
      return event.systemMetadata;
    }

    @Override
    protected String getEventDisplayString(TestEvent event) {
      return event.id;
    }
  }

  static final class TestEvent {
    final String id;
    SystemMetadata systemMetadata;

    TestEvent(String id) {
      this.id = id;
    }
  }

  abstract static class BaseHook implements EventHook<TestEvent> {
    boolean invoked;
    boolean failOnInvoke;
    String cascadeIdSeenDuringInvoke;

    @Override
    public boolean isEnabled() {
      return true;
    }

    @Override
    public void invoke(TestEvent event) throws Exception {
      invoked = true;
      cascadeIdSeenDuringInvoke = MDC.get(CascadeOperationContext.MDC_CASCADE_OPERATION_ID);
      if (failOnInvoke) {
        throw new RuntimeException("hook failed");
      }
    }
  }

  static final class HookOne extends BaseHook {}

  static final class HookTwo extends BaseHook {}
}
