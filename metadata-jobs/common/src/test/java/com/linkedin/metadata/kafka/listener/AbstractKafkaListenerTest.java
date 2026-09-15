package com.linkedin.metadata.kafka.listener;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.kafka.InboundMetadataEnvelope;
import com.linkedin.metadata.kafka.context.inbound.DefaultInboundBatchAffinityResolver;
import com.linkedin.metadata.kafka.context.inbound.InboundContextResolver;
import com.linkedin.metadata.utils.metrics.CascadeOperationContext;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.micrometer.core.instrument.Timer;
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

  private SimpleMeterRegistry meterRegistry;
  private StubListener listener;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    MDC.clear();
    meterRegistry = new SimpleMeterRegistry();
    when(metricUtils.getRegistry()).thenReturn(meterRegistry);
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
        operationContext,
        CONSUMER_GROUP,
        List.of(hook1, hook2),
        false,
        Collections.emptyMap(),
        mock(InboundContextResolver.class),
        new DefaultInboundBatchAffinityResolver());
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
  public void consumeInvokesHooks() throws Exception {
    TestEvent event = new TestEvent("evt-1");
    listener.nextEvent = event;

    ConsumerRecord<String, String> record = new ConsumerRecord<>(TOPIC, 0, 1L, "key", "payload");

    listener.consume(record);

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

    ConsumerRecord<String, String> record = new ConsumerRecord<>(TOPIC, 0, 0L, null, "bad");

    listener.consume(record);

    verify(metricUtils)
        .increment(eq(StubListener.class), eq(CONSUMER_GROUP + "_conversion_failure"), eq(1d));
    assertFalse(hook1.invoked);
  }

  @Test
  public void shouldSkipProcessingSkipsHooks() throws Exception {
    listener.skipNext = true;
    listener.nextEvent = new TestEvent("skip");

    ConsumerRecord<String, String> record = new ConsumerRecord<>(TOPIC, 0, 0L, null, "payload");

    listener.consume(record);

    assertFalse(hook1.invoked);
    verify(metricUtils, never())
        .increment(eq(StubListener.class), eq(CONSUMER_GROUP + "_consumed_event_count"), anyInt());
  }

  @Test
  public void hookFailureContinuesWithRemainingHooks() throws Exception {
    TestEvent event = new TestEvent("evt-hook-fail");
    listener.nextEvent = event;
    hook1.failOnInvoke = true;

    ConsumerRecord<String, String> record = new ConsumerRecord<>(TOPIC, 0, 0L, null, "payload");

    listener.consume(record);

    assertTrue(hook1.invoked);
    assertTrue(hook2.invoked);
    verify(metricUtils).increment(eq(StubListener.class), eq("HookOne_failure"), eq(1d));
  }

  /**
   * Direct {@code consumeEnvelope} entry — pgQueue path — must tag the lag timer with the pgQueue
   * messaging system AND the priority field from the envelope, and must thread the per-event {@link
   * OperationContext} returned by the resolver into the hook (not the system context). Without this
   * test the pgQueue funnel through {@code consumeEnvelope} relies on code symmetry with Kafka and
   * could regress silently (priority tag dropped, system context leaked to hooks).
   */
  @Test
  public void consumeEnvelopeFromPgQueueRoutesPerEventContextAndTagsPriority() throws Exception {
    // Distinct per-event context from the resolver — verify it threads into the hook instead of
    // the system context. This pins the resolver → hook wiring end-to-end.
    OperationContext perEventContext = mock(OperationContext.class);
    InboundContextResolver pgQueueResolver = mock(InboundContextResolver.class);
    when(pgQueueResolver.resolve(any(InboundMetadataEnvelope.class), eq(operationContext)))
        .thenReturn(perEventContext);
    listener.init(
        operationContext,
        CONSUMER_GROUP,
        List.of(hook1, hook2),
        false,
        Collections.emptyMap(),
        pgQueueResolver,
        new DefaultInboundBatchAffinityResolver());

    listener.nextEvent = new TestEvent("evt-pg");

    // Use a clearly-in-the-past enqueuedAt so that queueTimeMs (= now - enqueuedAt) inside
    // MetricUtils.recordInboundMessageQueueLag stays positive even under NTP correction or a slow
    // CI box. Micrometer's Timer.record(Duration) silently no-ops on negative amounts; a small
    // offset (e.g. 50ms) would otherwise let clock skew flip the diff negative and leave the
    // timer registered but with count=0, flaking the count assertion below.
    InboundMetadataEnvelope<String> envelope =
        InboundMetadataEnvelope.<String>builder()
            .logicalTopic(TOPIC)
            .messagingSystem(MetricUtils.MESSAGING_SYSTEM_PGQUEUE)
            .key("routing-key")
            .payload("payload")
            .enqueuedAtMillis(System.currentTimeMillis() - 60_000L)
            .consumerGroupId(CONSUMER_GROUP)
            .priority(2)
            .build();

    listener.consumeEnvelope(envelope);

    assertTrue(hook1.invoked);
    assertTrue(hook2.invoked);
    assertSame(
        hook1.contextSeenDuringInvoke,
        perEventContext,
        "Hook must see the per-event OperationContext returned by the resolver, not the system context");

    // pgQueue lag timer must carry both the messaging-system tag AND the priority tag.
    Timer pgTimer =
        meterRegistry
            .find(MetricUtils.MESSAGING_QUEUE_TIME)
            .tag(MetricUtils.MESSAGING_SYSTEM, MetricUtils.MESSAGING_SYSTEM_PGQUEUE)
            .tag(MetricUtils.MESSAGING_PRIORITY, "2")
            .timer();
    assertNotNull(
        pgTimer,
        "Expected lag timer tagged with messaging.system=pgqueue and messaging.priority=2");
    assertEquals(pgTimer.count(), 1);

    // Legacy Dropwizard histogram is still recorded alongside the Micrometer timer.
    verify(metricUtils).histogram(eq(StubListener.class), eq("kafkaLag"), anyLong());
  }

  /**
   * Direct {@code consumeEnvelope} entry — Kafka envelope built without going through {@code
   * consume(ConsumerRecord)}. Validates that the messaging-system tag is set to {@code kafka} and
   * that no {@code messaging.priority} tag is added (priority is a pgQueue concept; a Kafka
   * envelope must not produce one even if the field happens to be left blank).
   */
  @Test
  public void consumeEnvelopeFromKafkaTagsMessagingSystemAndOmitsPriority() throws Exception {
    InboundContextResolver passThrough = mock(InboundContextResolver.class);
    when(passThrough.resolve(any(InboundMetadataEnvelope.class), eq(operationContext)))
        .thenReturn(operationContext);
    listener.init(
        operationContext,
        CONSUMER_GROUP,
        List.of(hook1, hook2),
        false,
        Collections.emptyMap(),
        passThrough,
        new DefaultInboundBatchAffinityResolver());

    listener.nextEvent = new TestEvent("evt-kafka-direct");

    // Large past offset for the same reason as the pgQueue test — protects against negative
    // queueTimeMs from clock skew, which would leave Timer.count() at 0 and flake the assertion.
    InboundMetadataEnvelope<String> envelope =
        InboundMetadataEnvelope.<String>builder()
            .logicalTopic(TOPIC)
            .messagingSystem(MetricUtils.MESSAGING_SYSTEM_KAFKA)
            .key("k")
            .payload("payload")
            .enqueuedAtMillis(System.currentTimeMillis() - 60_000L)
            .consumerGroupId(CONSUMER_GROUP)
            .kafkaPartition(0)
            .kafkaOffset(1L)
            .serializedValueSize(7)
            // No priority — Kafka envelopes must not produce a priority tag downstream.
            .build();

    listener.consumeEnvelope(envelope);

    assertTrue(hook1.invoked);

    Timer kafkaTimer =
        meterRegistry
            .find(MetricUtils.MESSAGING_QUEUE_TIME)
            .tag(MetricUtils.MESSAGING_SYSTEM, MetricUtils.MESSAGING_SYSTEM_KAFKA)
            .timer();
    assertNotNull(kafkaTimer, "Expected lag timer tagged with messaging.system=kafka");
    assertEquals(kafkaTimer.count(), 1);

    // Every timer on this name registered for this test must lack the priority tag.
    meterRegistry
        .find(MetricUtils.MESSAGING_QUEUE_TIME)
        .timers()
        .forEach(
            t ->
                assertNull(
                    t.getId().getTag(MetricUtils.MESSAGING_PRIORITY),
                    "Kafka path must not emit messaging.priority tag, found tag on timer "
                        + t.getId()));
  }

  @Test
  public void cascadeOperationIdPropagatesToMdc() throws Exception {
    TestEvent event = new TestEvent("evt-cascade");
    StringMap props = new StringMap();
    props.put(CascadeOperationContext.SYSTEM_METADATA_CASCADE_ID_KEY, "cascade-123");
    event.systemMetadata = new SystemMetadata().setProperties(props);
    listener.nextEvent = event;

    ConsumerRecord<String, String> record = new ConsumerRecord<>(TOPIC, 0, 0L, null, "payload");

    listener.consume(record);

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
    OperationContext contextSeenDuringInvoke;

    @Override
    public boolean isEnabled() {
      return true;
    }

    @Override
    public void invoke(OperationContext operationContext, TestEvent event) throws Exception {
      invoked = true;
      contextSeenDuringInvoke = operationContext;
      cascadeIdSeenDuringInvoke = MDC.get(CascadeOperationContext.MDC_CASCADE_OPERATION_ID);
      if (failOnInvoke) {
        throw new RuntimeException("hook failed");
      }
    }
  }

  static final class HookOne extends BaseHook {}

  static final class HookTwo extends BaseHook {}
}
