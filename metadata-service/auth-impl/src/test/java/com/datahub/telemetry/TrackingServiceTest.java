package com.datahub.telemetry;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.kafka.TopicsConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.telemetry.TelemetryClientId;
import com.mixpanel.mixpanelapi.MessageBuilder;
import com.mixpanel.mixpanelapi.MixpanelAPI;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Optional;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TrackingServiceTest {
  private static final String APP_VERSION_FIELD = "appVersion";
  private static final String APP_VERSION = "1.0.0";
  private static final String CLIENT_ID = "testClientId";
  private static final TelemetryClientId TELEMETRY_CLIENT_ID =
      new TelemetryClientId().setClientId(CLIENT_ID);
  private static final String NOT_ALLOWED_FIELD = "browserId";
  private static final String NOT_ALLOWED_FIELD_VALUE = "testBrowserId";
  private static final String EVENT_TYPE_FIELD = "type";
  private static final String EVENT_TYPE = "TestEvent";
  private static final String FAILED_EVENT_TYPE = "FailedEvent";
  private static final String ACTOR_URN_FIELD = "actorUrn";
  private static final String ACTOR_URN_STRING = "urn:li:corpuser:user";
  private static final String HASHED_ACTOR_URN_STRING = "hashedActorUrn";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    OBJECT_MAPPER
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
  }

  private Urn _clientIdUrn;
  private JSONObject _mixpanelMessage;
  private MixpanelAPI _mixpanelAPI;
  private MessageBuilder _mixpanelMessageBuilder;
  private SecretService _secretService;
  private EntityService<?> _entityService;
  private TrackingService _trackingService;
  private OperationContext opContext;
  private Producer<String, String> dataHubUsageProducer;
  private TrackingService _noObfuscationTrackingService;

  @BeforeMethod
  public void setupTest() {
    _clientIdUrn = UrnUtils.getUrn(CLIENT_ID_URN);
    _mixpanelAPI = mock(MixpanelAPI.class);
    _mixpanelMessageBuilder = mock(MessageBuilder.class);
    _mixpanelMessage = spy(new JSONObject());
    _mixpanelMessage.put("properties", new JSONObject());
    when(_mixpanelMessageBuilder.event(anyString(), anyString(), any(JSONObject.class)))
        .thenReturn(_mixpanelMessage);
    _secretService = mock(SecretService.class);
    _entityService = mock(EntityService.class);
    GitVersion gitVersion = new GitVersion(APP_VERSION, "", Optional.empty());

    TopicsConfiguration topicsConfiguration = mock(TopicsConfiguration.class);
    when(topicsConfiguration.getDataHubUsage()).thenReturn("DataHubUsageEvent_v1");

    dataHubUsageProducer = mock(Producer.class);
    when(dataHubUsageProducer.send(any(ProducerRecord.class), any()))
        .thenAnswer(
            invocation -> {
              // Get the callback from the second argument
              org.apache.kafka.clients.producer.Callback callback = invocation.getArgument(1);
              TopicPartition partition = new TopicPartition("DataHubUsageEvent_v1", 1);
              RecordMetadata metadata = new RecordMetadata(partition, 0, 0, 0, 0, 0);
              callback.onCompletion(metadata, null);
              return null;
            });

    // Mock the operation context
    opContext = mock(OperationContext.class);
    io.datahubproject.metadata.context.ActorContext actorContext =
        mock(io.datahubproject.metadata.context.ActorContext.class);
    when(opContext.getActorContext()).thenReturn(actorContext);
    when(actorContext.getActorUrn()).thenReturn(UrnUtils.getUrn(ACTOR_URN_STRING));

    _trackingService =
        new TrackingService(
            topicsConfiguration,
            _secretService,
            _mixpanelMessageBuilder,
            _mixpanelAPI,
            _entityService,
            gitVersion,
            dataHubUsageProducer);
  }

  @Test
  public void testTrackEvent() throws IOException {
    // Mock the secret service
    when(_secretService.hashString(eq(ACTOR_URN_STRING))).thenReturn(HASHED_ACTOR_URN_STRING);
    when(_entityService.exists(any(OperationContext.class), eq(_clientIdUrn), eq(true)))
        .thenReturn(true);
    when(_entityService.getLatestAspect(
            any(OperationContext.class), eq(_clientIdUrn), eq(CLIENT_ID_ASPECT)))
        .thenReturn(TELEMETRY_CLIENT_ID);

    // Create a test event with nested event object containing timestamp
    JSONObject testEvent = new JSONObject();
    testEvent.put("type", EVENT_TYPE);
    testEvent.put("entityType", "dataset");
    testEvent.put(
        "entityUrn", "urn:li:dataset:(urn:li:dataPlatform:bigquery,example_dataset,PROD)");
    testEvent.put("actorUrn", ACTOR_URN_STRING);
    testEvent.put("customField", "test_value");
    testEvent.put("timestamp", "2025-04-05T15:35:03.506874+00:00");

    // Add nested event object with timestamp to cover the specific code path
    JSONObject nestedEvent = new JSONObject();
    nestedEvent.put("timestamp", "2025-04-05T15:30:00.000000+00:00");
    nestedEvent.put("nestedField", "nested_value");
    testEvent.put("event", nestedEvent);

    // Convert to JsonNode
    com.fasterxml.jackson.databind.JsonNode eventNode =
        OBJECT_MAPPER.readTree(testEvent.toString());

    // Mock the opContext behavior
    when(opContext.getActorContext().getActorUrn()).thenReturn(UrnUtils.getUrn(ACTOR_URN_STRING));

    // Call the track method directly
    _trackingService.track(EVENT_TYPE, opContext, null, null, eventNode);

    // Verify that the Mixpanel API was called
    verify(_mixpanelAPI, times(1)).sendMessage(any(JSONObject.class));

    // Verify that the Kafka producer was called
    verify(dataHubUsageProducer, times(1)).send(any(ProducerRecord.class), any());

    // For testing the empty destination path
    _trackingService.track(EVENT_TYPE, opContext, null, null, eventNode, Collections.emptySet());
  }

  @Test
  public void testTrackEventWithKafkaError() throws IOException {
    // Create a new producer that simulates an error
    Producer<String, String> errorProducer = mock(Producer.class);
    when(errorProducer.send(any(ProducerRecord.class), any()))
        .thenAnswer(
            invocation -> {
              // Get the callback from the second argument
              org.apache.kafka.clients.producer.Callback callback = invocation.getArgument(1);
              // Execute the callback with an error (null metadata, non-null exception)
              callback.onCompletion(null, new RuntimeException("Kafka error"));
              return null;
            });

    // Create a new tracking service with the error producer
    TopicsConfiguration topicsConfiguration = mock(TopicsConfiguration.class);
    when(topicsConfiguration.getDataHubUsage()).thenReturn("DataHubUsageEvent_v1");

    GitVersion gitVersion = new GitVersion(APP_VERSION, "", Optional.empty());
    TrackingService errorTrackingService =
        new TrackingService(
            topicsConfiguration,
            _secretService,
            _mixpanelMessageBuilder,
            _mixpanelAPI,
            _entityService,
            gitVersion,
            errorProducer);

    // Mock the secret service
    when(_secretService.hashString(eq(ACTOR_URN_STRING))).thenReturn(HASHED_ACTOR_URN_STRING);
    when(_entityService.exists(any(OperationContext.class), eq(_clientIdUrn), eq(true)))
        .thenReturn(true);
    when(_entityService.getLatestAspect(
            any(OperationContext.class), eq(_clientIdUrn), eq(CLIENT_ID_ASPECT)))
        .thenReturn(TELEMETRY_CLIENT_ID);

    // Create a test event
    JSONObject testEvent = new JSONObject();
    testEvent.put("type", EVENT_TYPE);
    testEvent.put("actorUrn", ACTOR_URN_STRING);

    // Convert to JsonNode
    com.fasterxml.jackson.databind.JsonNode eventNode =
        OBJECT_MAPPER.readTree(testEvent.toString());

    // Mock the opContext behavior
    when(opContext.getActorContext().getActorUrn()).thenReturn(UrnUtils.getUrn(ACTOR_URN_STRING));

    // Call the track method - it should handle the error gracefully
    int numDestinations = errorTrackingService.track(EVENT_TYPE, opContext, null, null, eventNode);

    // Verify that the Kafka producer was called
    verify(errorProducer, times(1)).send(any(ProducerRecord.class), any());

    assertEquals(numDestinations, 2);
  }

  @Test
  public void testTrackEventKafkaOnly() throws IOException {
    // Mock the secret service
    when(_secretService.hashString(eq(ACTOR_URN_STRING))).thenReturn(HASHED_ACTOR_URN_STRING);
    when(_entityService.exists(any(OperationContext.class), eq(_clientIdUrn), eq(true)))
        .thenReturn(true);
    when(_entityService.getLatestAspect(
            any(OperationContext.class), eq(_clientIdUrn), eq(CLIENT_ID_ASPECT)))
        .thenReturn(TELEMETRY_CLIENT_ID);

    // Create a test event
    JSONObject testEvent = new JSONObject();
    testEvent.put("type", EVENT_TYPE);
    testEvent.put("actorUrn", ACTOR_URN_STRING);

    // Convert to JsonNode
    com.fasterxml.jackson.databind.JsonNode eventNode =
        OBJECT_MAPPER.readTree(testEvent.toString());

    // Mock the opContext behavior
    when(opContext.getActorContext().getActorUrn()).thenReturn(UrnUtils.getUrn(ACTOR_URN_STRING));

    // Call the track method with only Kafka destination
    int numDestinations =
        _trackingService.track(
            EVENT_TYPE, opContext, null, null, eventNode, EnumSet.of(TrackingDestination.KAFKA));

    // Verify that the Mixpanel API was NOT called
    verify(_mixpanelAPI, never()).sendMessage(any(JSONObject.class));

    // Verify that the Kafka producer was called
    verify(dataHubUsageProducer, times(1)).send(any(ProducerRecord.class), any());

    // Should return 1 for Kafka destination
    assertEquals(numDestinations, 1);
  }

  @Test
  public void testGetClientIdAlreadyExists() {
    when(_entityService.exists(any(OperationContext.class), eq(_clientIdUrn), eq(true)))
        .thenReturn(true);
    when(_entityService.getLatestAspect(
            any(OperationContext.class), eq(_clientIdUrn), eq(CLIENT_ID_ASPECT)))
        .thenReturn(TELEMETRY_CLIENT_ID);

    assertEquals(CLIENT_ID, _trackingService.getClientId(opContext));

    assertEquals(
        CLIENT_ID,
        _trackingService.getClientId(opContext)); // Second call to get through the cached path
  }

  @Test
  public void testGetClientIdDoesNotExist() {
    when(_entityService.exists(any(OperationContext.class), eq(_clientIdUrn), eq(true)))
        .thenReturn(false);

    assertNotNull(_trackingService.getClientId(opContext));
    verify(_entityService, times(1))
        .ingestAspectIfNotPresent(
            any(OperationContext.class),
            eq(_clientIdUrn),
            eq(CLIENT_ID_ASPECT),
            any(TelemetryClientId.class),
            any(),
            eq(null));
  }

  @Test
  public void testSanitizeEventNoEventType() throws JsonProcessingException, JSONException {
    final JSONObject event = new JSONObject();
    event.put(ACTOR_URN_FIELD, ACTOR_URN_STRING);
    event.put(NOT_ALLOWED_FIELD, NOT_ALLOWED_FIELD_VALUE);

    final JSONObject sanitizedEvent = _trackingService.sanitizeEvent(event);
    assertNotNull(sanitizedEvent);
    assertTrue(sanitizedEvent.has(APP_VERSION_FIELD));
    assertEquals(sanitizedEvent.get(APP_VERSION_FIELD), APP_VERSION);
    assertTrue(sanitizedEvent.has(EVENT_TYPE_FIELD));
    assertEquals(sanitizedEvent.get(EVENT_TYPE_FIELD), FAILED_EVENT_TYPE);
  }

  @Test
  public void testSanitizeEventNoActorUrn() throws JsonProcessingException, JSONException {
    final JSONObject event = new JSONObject();
    event.put(EVENT_TYPE_FIELD, EVENT_TYPE);
    event.put(NOT_ALLOWED_FIELD, NOT_ALLOWED_FIELD_VALUE);

    final JSONObject sanitizedEvent = _trackingService.sanitizeEvent(event);
    assertNotNull(sanitizedEvent);
    assertTrue(sanitizedEvent.has(APP_VERSION_FIELD));
    assertEquals(sanitizedEvent.get(APP_VERSION_FIELD), APP_VERSION);
    assertTrue(sanitizedEvent.has(EVENT_TYPE_FIELD));
    assertEquals(sanitizedEvent.get(EVENT_TYPE_FIELD), EVENT_TYPE);
    assertFalse(sanitizedEvent.has(ACTOR_URN_FIELD));
    assertFalse(sanitizedEvent.has(NOT_ALLOWED_FIELD));
  }

  @Test
  public void testSanitizeEvent() throws JsonProcessingException, JSONException {

    reset(_secretService);
    when(_secretService.hashString(eq(ACTOR_URN_STRING))).thenReturn(HASHED_ACTOR_URN_STRING);
    final JSONObject event = new JSONObject();
    event.put(EVENT_TYPE_FIELD, EVENT_TYPE);
    event.put(ACTOR_URN_FIELD, ACTOR_URN_STRING);
    event.put(NOT_ALLOWED_FIELD, NOT_ALLOWED_FIELD_VALUE);

    final JSONObject sanitizedEvent = _trackingService.sanitizeEvent(event);
    assertNotNull(sanitizedEvent);
    assertTrue(sanitizedEvent.has(APP_VERSION_FIELD));
    assertEquals(sanitizedEvent.get(APP_VERSION_FIELD), APP_VERSION);
    assertTrue(sanitizedEvent.has(EVENT_TYPE_FIELD));
    assertEquals(sanitizedEvent.get(EVENT_TYPE_FIELD), EVENT_TYPE);
    assertTrue(sanitizedEvent.has(ACTOR_URN_FIELD));
    assertEquals(sanitizedEvent.get(ACTOR_URN_FIELD), HASHED_ACTOR_URN_STRING);
    assertFalse(sanitizedEvent.has(NOT_ALLOWED_FIELD));
  }

  @Test
  public void testTrackEventWithNestedEventTimestamp() throws IOException {
    // Mock the secret service
    when(_secretService.hashString(eq(ACTOR_URN_STRING))).thenReturn(HASHED_ACTOR_URN_STRING);
    when(_entityService.exists(any(OperationContext.class), eq(_clientIdUrn), eq(true)))
        .thenReturn(true);
    when(_entityService.getLatestAspect(
            any(OperationContext.class), eq(_clientIdUrn), eq(CLIENT_ID_ASPECT)))
        .thenReturn(TELEMETRY_CLIENT_ID);

    // Create a test event with nested event object containing timestamp
    JSONObject testEvent = new JSONObject();
    testEvent.put("type", EVENT_TYPE);
    testEvent.put("actorUrn", ACTOR_URN_STRING);
    testEvent.put("timestamp", "2025-04-05T15:35:03.506874+00:00");

    // Add nested event object with timestamp to specifically test the code path
    JSONObject nestedEvent = new JSONObject();
    nestedEvent.put("timestamp", "2025-04-05T15:30:00.000000+00:00");
    nestedEvent.put("nestedField", "nested_value");
    testEvent.put("event", nestedEvent);

    // Convert to JsonNode
    com.fasterxml.jackson.databind.JsonNode eventNode =
        OBJECT_MAPPER.readTree(testEvent.toString());

    // Mock the opContext behavior
    when(opContext.getActorContext().getActorUrn()).thenReturn(UrnUtils.getUrn(ACTOR_URN_STRING));

    // Call the track method with only Kafka destination to focus on the nested timestamp update
    int numDestinations =
        _trackingService.track(
            EVENT_TYPE, opContext, null, null, eventNode, EnumSet.of(TrackingDestination.KAFKA));

    // Verify that the Kafka producer was called
    verify(dataHubUsageProducer, times(1)).send(any(ProducerRecord.class), any());

    // Should return 1 for Kafka destination
    assertEquals(numDestinations, 1);
  }

  @Test
  public void testParseTimestampNull() throws Exception {
    // Test null timestamp - should return current time
    long result = _trackingService.parseTimestamp(null);
    assertTrue(result > 0);
    assertTrue(result <= System.currentTimeMillis());
  }

  @Test
  public void testParseTimestampNumberMilliseconds() throws Exception {
    // Test number timestamp in milliseconds (> 1e12)
    long timestamp = 1649160000000L;
    long result = _trackingService.parseTimestamp(timestamp);
    assertEquals(result, timestamp);
  }

  @Test
  public void testParseTimestampNumberSeconds() throws Exception {
    // Test number timestamp in seconds (< 1e12)
    long timestampSeconds = 1649160000L;
    long expectedMillis = timestampSeconds * 1000;
    long result = _trackingService.parseTimestamp(timestampSeconds);
    assertEquals(result, expectedMillis);
  }

  @Test
  public void testParseTimestampStringISO() throws Exception {
    // Test ISO 8601 string timestamp
    String isoTimestamp = "2022-04-05T12:00:00Z";
    long result = _trackingService.parseTimestamp(isoTimestamp);
    // Expected: 1649160000000L for "2022-04-05T12:00:00Z"
    assertEquals(result, 1649160000000L);
  }

  @Test
  public void testParseTimestampStringNumberMilliseconds() throws Exception {
    // Test string number timestamp in milliseconds
    String timestampStr = "1649160000000";
    long result = _trackingService.parseTimestamp(timestampStr);
    assertEquals(result, 1649160000000L);
  }

  @Test
  public void testParseTimestampStringNumberSeconds() throws Exception {
    // Test string number timestamp in seconds
    String timestampStr = "1649160000";
    long expectedMillis = 1649160000L * 1000;
    long result = _trackingService.parseTimestamp(timestampStr);
    assertEquals(result, expectedMillis);
  }

  @Test
  public void testParseTimestampStringInvalid() throws Exception {
    // Test invalid string timestamp - should return current time
    String invalidTimestamp = "invalid-timestamp";
    long result = _trackingService.parseTimestamp(invalidTimestamp);
    assertTrue(result > 0);
    assertTrue(result <= System.currentTimeMillis());
  }

  @Test
  public void testParseTimestampUnsupportedType() throws Exception {
    // Test unsupported type - should return current time
    Object unsupportedType = new Object();
    long result = _trackingService.parseTimestamp(unsupportedType);
    assertTrue(result > 0);
    assertTrue(result <= System.currentTimeMillis());
  }

  @Test
  public void testCreateFailedEvent() throws Exception {
    // Test createFailedEvent method
    JSONObject failedEvent = _trackingService.createFailedEvent();

    // Verify the failed event is not null
    assertNotNull(failedEvent);

    // Verify it contains the required fields
    assertTrue(failedEvent.has(APP_VERSION_FIELD));
    assertEquals(failedEvent.get(APP_VERSION_FIELD), APP_VERSION);

    assertTrue(failedEvent.has(EVENT_TYPE_FIELD));
    assertEquals(failedEvent.get(EVENT_TYPE_FIELD), FAILED_EVENT_TYPE);

    // Verify it only contains these two fields
    assertEquals(failedEvent.length(), 2);
  }

  @Test
  public void testTransformObjectNodeToJSONObjectSuccess() throws Exception {
    // Test successful transformation of ObjectNode to JSONObject
    ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
    objectNode.put("testField", "testValue");
    objectNode.put("numberField", 123);
    objectNode.put("booleanField", true);

    JSONObject result = _trackingService.transformObjectNodeToJSONObject(objectNode);

    // Verify the result is not null
    assertNotNull(result);

    // Verify all fields are preserved
    assertTrue(result.has("testField"));
    assertEquals(result.get("testField"), "testValue");

    assertTrue(result.has("numberField"));
    assertEquals(result.get("numberField"), 123);

    assertTrue(result.has("booleanField"));
    assertEquals(result.get("booleanField"), true);

    // Verify the number of fields matches
    assertEquals(result.length(), 3);
  }

  @Test
  public void testTransformObjectNodeToJSONObjectException() throws Exception {
    // Test the exception path where serialization fails and method returns null
    // We'll use reflection to replace the ObjectWriter with one that throws an exception

    // Create a simple ObjectNode
    ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
    objectNode.put("testField", "testValue");

    // Use reflection to temporarily replace the ObjectWriter with one that throws an exception
    java.lang.reflect.Field objectWriterField =
        TrackingService.class.getDeclaredField("_objectWriter");
    objectWriterField.setAccessible(true);

    // Save the original ObjectWriter
    ObjectWriter originalWriter = (ObjectWriter) objectWriterField.get(_trackingService);

    try {
      // Create a mock ObjectWriter that throws an exception
      ObjectWriter mockWriter = mock(ObjectWriter.class);
      when(mockWriter.writeValueAsString(any()))
          .thenThrow(new RuntimeException("Serialization failed"));

      // Replace the ObjectWriter
      objectWriterField.set(_trackingService, mockWriter);

      // Call the method - it should return null due to the exception
      JSONObject result = _trackingService.transformObjectNodeToJSONObject(objectNode);

      // Verify the result is null due to the exception
      assertNull(result);

    } finally {
      // Restore the original ObjectWriter
      objectWriterField.set(_trackingService, originalWriter);
    }
  }

  @Test
  public void testTrackEventWithNoServices() throws IOException {
    // Create a tracking service without producer and MixpanelAPI to cover log paths
    TopicsConfiguration topicsConfiguration = mock(TopicsConfiguration.class);
    when(topicsConfiguration.getDataHubUsage()).thenReturn("DataHubUsageEvent_v1");

    GitVersion gitVersion = new GitVersion(APP_VERSION, "", Optional.empty());
    TrackingService noServicesTrackingService =
        new TrackingService(
            topicsConfiguration,
            _secretService,
            null, // No MessageBuilder
            null, // No MixpanelAPI
            _entityService,
            gitVersion,
            null); // No Kafka producer

    // Mock the secret service
    when(_secretService.hashString(eq(ACTOR_URN_STRING))).thenReturn(HASHED_ACTOR_URN_STRING);
    when(_entityService.exists(any(OperationContext.class), eq(_clientIdUrn), eq(true)))
        .thenReturn(true);
    when(_entityService.getLatestAspect(
            any(OperationContext.class), eq(_clientIdUrn), eq(CLIENT_ID_ASPECT)))
        .thenReturn(TELEMETRY_CLIENT_ID);

    // Create a test event
    JSONObject testEvent = new JSONObject();
    testEvent.put("type", EVENT_TYPE);
    testEvent.put("actorUrn", ACTOR_URN_STRING);

    // Convert to JsonNode
    com.fasterxml.jackson.databind.JsonNode eventNode =
        OBJECT_MAPPER.readTree(testEvent.toString());

    // Mock the opContext behavior
    when(opContext.getActorContext().getActorUrn()).thenReturn(UrnUtils.getUrn(ACTOR_URN_STRING));

    // Call the track method with Mixpanel destination - should log warning and skip
    int numDestinations =
        noServicesTrackingService.track(
            EVENT_TYPE, opContext, null, null, eventNode, EnumSet.of(TrackingDestination.MIXPANEL));

    // Should return 0 since no services are available
    assertEquals(numDestinations, 0);

    // Call the track method with Kafka destination - should log warning and skip
    numDestinations =
        noServicesTrackingService.track(
            EVENT_TYPE, opContext, null, null, eventNode, EnumSet.of(TrackingDestination.KAFKA));

    // Should return 0 since no services are available
    assertEquals(numDestinations, 0);

    // Call the track method with both destinations - should log warnings and skip both
    numDestinations = noServicesTrackingService.track(EVENT_TYPE, opContext, null, null, eventNode);

    // Should return 0 since no services are available
    assertEquals(numDestinations, 0);
  }
}
