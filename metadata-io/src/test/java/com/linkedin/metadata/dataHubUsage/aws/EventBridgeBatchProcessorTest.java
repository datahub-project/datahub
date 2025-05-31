package com.linkedin.metadata.dataHubUsage.aws;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.config.aws.EventBridgeConfiguration;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResultEntry;

@Slf4j
public class EventBridgeBatchProcessorTest {

  private static final String TEST_EVENT_BUS = "test-event-bus";
  private static final String TEST_REGION = "us-west-2";
  private static final int MAX_BATCH_SIZE = 5;
  private static final int MAX_RETRIES = 3;
  private static final int FLUSH_INTERVAL = 10;

  @Mock private EventBridgeClient mockEventBridgeClient;
  @Mock private ScheduledExecutorService mockScheduler;

  private AutoCloseable mocks;
  private EventBridgeBatchProcessor processor;
  private final ObjectMapper mapper = new ObjectMapper();

  @BeforeMethod
  public void setup() throws Exception {
    mocks = MockitoAnnotations.openMocks(this);

    // Create mock configuration
    EventBridgeConfiguration config = new EventBridgeConfiguration();
    config.setEventBus(TEST_EVENT_BUS);
    config.setRegion(TEST_REGION);
    config.setMaxBatchSize(MAX_BATCH_SIZE);
    config.setMaxRetries(MAX_RETRIES);
    config.setFlushIntervalSeconds(FLUSH_INTERVAL);

    // Use test implementation with mocked dependencies
    processor = new TestEventBridgeBatchProcessor(config, mockEventBridgeClient, mockScheduler);

    // Setup default mock behavior
    PutEventsResponse mockResponse = mock(PutEventsResponse.class);
    when(mockResponse.failedEntryCount()).thenReturn(0);
    when(mockEventBridgeClient.putEvents(any(PutEventsRequest.class))).thenReturn(mockResponse);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (processor != null) {
      processor.close();
    }
    if (mocks != null) {
      mocks.close();
    }
  }

  @Test
  public void testAddSingleEventDoesNotTriggerFlush() {
    // When adding a single event
    processor.addEvent(createSampleEvent("test-event-1"), "TEST_EVENT");

    // Then flush should not be triggered
    verify(mockEventBridgeClient, never()).putEvents(any(PutEventsRequest.class));
  }

  @Test
  public void testAddEventsUpToMaxBatchSizeTriggersFlush() {
    // When adding events up to the max batch size
    for (int i = 0; i < MAX_BATCH_SIZE; i++) {
      processor.addEvent(createSampleEvent("test-event-" + i), "TEST_EVENT");
    }

    // Then flush should be triggered once
    verify(mockEventBridgeClient, times(1)).putEvents(any(PutEventsRequest.class));
  }

  @Test
  public void testFlushSendsCorrectEvents() {
    // Given multiple events
    String eventType = "TEST_EVENT";
    for (int i = 0; i < 3; i++) {
      processor.addEvent(createSampleEvent("test-event-" + i), eventType);
    }

    // When flush is called
    processor.flush();

    // Then the correct events should be sent
    ArgumentCaptor<PutEventsRequest> requestCaptor =
        ArgumentCaptor.forClass(PutEventsRequest.class);
    verify(mockEventBridgeClient, times(1)).putEvents(requestCaptor.capture());

    PutEventsRequest capturedRequest = requestCaptor.getValue();
    List<PutEventsRequestEntry> entries = capturedRequest.entries();

    // Verify request entries
    org.testng.Assert.assertEquals(entries.size(), 3);
    for (int i = 0; i < entries.size(); i++) {
      PutEventsRequestEntry entry = entries.get(i);
      assertEquals(entry.source(), "acryl.events");
      assertEquals(entry.detailType(), eventType);
      assertEquals(entry.eventBusName(), TEST_EVENT_BUS);
      // Note: We're not verifying exact JSON content here, just that it exists
      assertTrue(entry.detail().contains("test-event-" + i));
    }
  }

  @Test
  public void testEmptyBufferDoesNotSendRequest() {
    processor.flush();

    verify(mockEventBridgeClient, never()).putEvents(any(PutEventsRequest.class));
  }

  @Test
  public void testRetryOnFailedEntries() {
    // Given a response with some failed entries
    PutEventsResponse mockResponseWithFailures = mock(PutEventsResponse.class);
    when(mockResponseWithFailures.failedEntryCount()).thenReturn(2);

    List<PutEventsResultEntry> resultEntries = new ArrayList<>();
    // First entry succeeds
    resultEntries.add(PutEventsResultEntry.builder().build());
    // Second entry fails
    resultEntries.add(PutEventsResultEntry.builder().errorCode("ThrottlingException").build());
    // Third entry fails
    resultEntries.add(PutEventsResultEntry.builder().errorCode("InternalException").build());

    when(mockResponseWithFailures.entries()).thenReturn(resultEntries);
    when(mockEventBridgeClient.putEvents(any(PutEventsRequest.class)))
        .thenReturn(mockResponseWithFailures)
        .thenReturn(mock(PutEventsResponse.class)); // Second call succeeds

    for (int i = 0; i < 3; i++) {
      processor.addEvent(createSampleEvent("test-event-" + i), "TEST_EVENT");
    }
    processor.flush();

    verify(mockEventBridgeClient, times(1)).putEvents(any(PutEventsRequest.class));

    verify(mockScheduler, times(1))
        .schedule(any(Runnable.class), Mockito.anyLong(), Mockito.eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void testRetryWithExponentialBackoff() {
    // Given a client that throws an exception
    when(mockEventBridgeClient.putEvents(any(PutEventsRequest.class)))
        .thenThrow(new RuntimeException("Test exception"));

    // When adding an event and flushing
    processor.addEvent(createSampleEvent("test-event"), "TEST_EVENT");
    processor.flush();

    // Then retry should be scheduled with exponential backoff
    verify(mockScheduler, times(1))
        .schedule(
            any(Runnable.class),
            Mockito.eq(100L), // 2^0 * 100ms for first retry
            Mockito.eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void testCloseFlushesAndShutsDownScheduler() {
    // Given some events in the buffer
    processor.addEvent(createSampleEvent("test-event"), "TEST_EVENT");

    // When close is called
    processor.close();

    // Then buffer should be flushed
    verify(mockEventBridgeClient, times(1)).putEvents(any(PutEventsRequest.class));

    // And scheduler should be shut down
    verify(mockScheduler, times(1)).shutdown();
  }

  /** Creates a sample JsonNode event for testing with the actual event structure. */
  private JsonNode createSampleEvent(String id) {
    ObjectNode node = mapper.createObjectNode();
    node.put("actorUrn", "urn:li:corpuser:__datahub_system");
    node.put("type", "CreateUserEvent");
    node.put("timestamp", System.currentTimeMillis());
    node.put("entityUrn", "urn:li:corpuser:" + id);
    node.put("entityType", "corpuser");
    node.put("traceId", "123456");
    node.put("aspectName", "corpUserKey");
    node.put("usageSource", "backend");
    node.put("@timestamp", System.currentTimeMillis());
    return node;
  }

  /** Test implementation of EventBridgeBatchProcessor that allows injecting mocks. */
  private static class TestEventBridgeBatchProcessor extends EventBridgeBatchProcessor {
    public TestEventBridgeBatchProcessor(
        EventBridgeConfiguration config,
        EventBridgeClient eventBridgeClient,
        ScheduledExecutorService scheduler) {
      super(config);

      // Use reflection to inject our mocks
      try {
        Field clientField = EventBridgeBatchProcessor.class.getDeclaredField("eventBridgeClient");
        clientField.setAccessible(true);
        clientField.set(this, eventBridgeClient);

        Field schedulerField = EventBridgeBatchProcessor.class.getDeclaredField("scheduler");
        schedulerField.setAccessible(true);
        schedulerField.set(this, scheduler);
      } catch (Exception e) {
        throw new RuntimeException("Failed to inject mocks", e);
      }
    }
  }
}
