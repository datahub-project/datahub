package com.linkedin.gms.factory.system_telemetry.usage;

import static com.linkedin.metadata.datahubusage.DataHubUsageEventConstants.*;
import static com.linkedin.metadata.datahubusage.DataHubUsageEventConstants.EVENT_SOURCE;
import static com.linkedin.metadata.datahubusage.DataHubUsageEventConstants.SOURCE_IP;
import static com.linkedin.metadata.datahubusage.DataHubUsageEventType.DELETE_ENTITY_EVENT;
import static com.linkedin.metadata.telemetry.OpenTelemetryKeyConstants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.UsageExportConfiguration;
import com.linkedin.metadata.datahubusage.DataHubUsageEventType;
import com.linkedin.metadata.telemetry.OpenTelemetryKeyConstants;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataHubUsageSpanExporterTest {

  private Producer<String, String> mockProducer;
  private UsageExportConfiguration config;
  private DataHubUsageSpanExporter exporter;
  private final String TEST_TOPIC = "test-usage-topic";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @BeforeMethod
  public void setup() {
    mockProducer = mock(Producer.class);
    config = new UsageExportConfiguration();
    config.setUsageEventTypes("LOG_IN_EVENT,SEARCH_RESULT_CLICK_EVENT");
    config.setAspectTypes("aspect1,aspect2");
    config.setUserFilters("urn:li:corpuser:blacklisted");
    exporter = new DataHubUsageSpanExporter(mockProducer, TEST_TOPIC, config);
  }

  @Test
  public void testExportLoginEvent() {
    // Prepare test data
    String userId = "urn:li:corpuser:testUser";
    String loginSource = "PASSWORD_LOGIN";
    String userAgent = "Mozilla/5.0";
    String sourceIp = "192.168.1.1";
    String traceId = "trace123";

    // Create login event
    EventData loginEvent = createLoginEvent(userId, loginSource, userAgent, sourceIp, traceId);

    // Create a SpanData containing the event
    SpanData spanData = mock(SpanData.class);
    when(spanData.getEvents()).thenReturn(Collections.singletonList(loginEvent));

    // Export the span
    CompletableResultCode result = exporter.export(Collections.singletonList(spanData));

    // Verify result is successful
    assertTrue(result.isSuccess());

    // Capture Kafka record
    ArgumentCaptor<ProducerRecord<String, String>> recordCaptor =
        ArgumentCaptor.forClass(ProducerRecord.class);
    verify(mockProducer).send(recordCaptor.capture());

    // Verify Kafka record properties
    ProducerRecord<String, String> record = recordCaptor.getValue();
    assertEquals(TEST_TOPIC, record.topic());
    assertEquals(userId, record.key());

    // Verify event payload
    try {
      JsonNode eventJson = OBJECT_MAPPER.readTree(record.value());
      assertEquals(userId, eventJson.get(ACTOR_URN).asText());
      assertEquals(DataHubUsageEventType.LOG_IN_EVENT.getType(), eventJson.get(TYPE).asText());
      assertTrue(eventJson.has(TIMESTAMP));
      assertEquals(loginSource, eventJson.get(LOGIN_SOURCE).asText());
      assertEquals(userAgent, eventJson.get(USER_AGENT).asText());
      assertEquals(sourceIp, eventJson.get(SOURCE_IP).asText());
      assertEquals(traceId, eventJson.get(TRACE_ID).asText());
      assertEquals(BACKEND_SOURCE, eventJson.get(USAGE_SOURCE).asText());
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse JSON", e);
    }
  }

  @Test
  public void testExportUpdateAspectEvent() {
    // Prepare test data
    String userId = "urn:li:corpuser:testUser";
    String entityUrn = "urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)";
    String entityType = "dataset";
    String aspectName = "aspect1"; // This is in the config
    String userAgent = "Mozilla/5.0";
    String sourceIp = "192.168.1.1";
    String eventSource = "GRAPHQL";

    // Create update aspect event
    EventData updateEvent =
        createUpdateAspectEvent(
            userId, entityUrn, entityType, aspectName, eventSource, userAgent, sourceIp);

    // Create a SpanData containing the event
    SpanData spanData = mock(SpanData.class);
    when(spanData.getEvents()).thenReturn(Collections.singletonList(updateEvent));

    // Export the span
    CompletableResultCode result = exporter.export(Collections.singletonList(spanData));

    // Verify result is successful
    assertTrue(result.isSuccess());

    // Capture Kafka record
    ArgumentCaptor<ProducerRecord<String, String>> recordCaptor =
        ArgumentCaptor.forClass(ProducerRecord.class);
    verify(mockProducer).send(recordCaptor.capture());

    // Verify Kafka record properties
    ProducerRecord<String, String> record = recordCaptor.getValue();
    assertEquals(TEST_TOPIC, record.topic());
    assertEquals(userId, record.key());

    // Verify event payload
    try {
      JsonNode eventJson = OBJECT_MAPPER.readTree(record.value());
      assertEquals(userId, eventJson.get(ACTOR_URN).asText());
      assertEquals(
          DataHubUsageEventType.UPDATE_ASPECT_EVENT.getType(), eventJson.get(TYPE).asText());
      assertTrue(eventJson.has(TIMESTAMP));
      assertEquals(entityUrn, eventJson.get(ENTITY_URN).asText());
      assertEquals(entityType, eventJson.get(ENTITY_TYPE).asText());
      assertEquals(aspectName, eventJson.get(ASPECT_NAME).asText());
      assertEquals(eventSource, eventJson.get(EVENT_SOURCE).asText());
      assertEquals(userAgent, eventJson.get(USER_AGENT).asText());
      assertEquals(sourceIp, eventJson.get(SOURCE_IP).asText());
      assertEquals(BACKEND_SOURCE, eventJson.get(USAGE_SOURCE).asText());
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse JSON", e);
    }
  }

  @Test
  public void testExportConfiguredEventType() {
    // Prepare test data
    String userId = "urn:li:corpuser:testUser";
    String entityUrn = "urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)";
    String entityType = "dataset";

    // Create search click event (in config.usageEventTypes)
    EventData searchClickEvent =
        createGenericEvent(
            UPDATE_ASPECT_EVENT,
            userId,
            DataHubUsageEventType.SEARCH_RESULT_CLICK_EVENT.getType(),
            entityUrn,
            entityType,
            null,
            null,
            null,
            null);

    // Create a SpanData containing the event
    SpanData spanData = mock(SpanData.class);
    when(spanData.getEvents()).thenReturn(Collections.singletonList(searchClickEvent));

    // Export the span
    CompletableResultCode result = exporter.export(Collections.singletonList(spanData));

    // Verify result is successful
    assertTrue(result.isSuccess());

    // Verify event was published
    verify(mockProducer).send(any(ProducerRecord.class));
  }

  @Test
  public void testFilterBlacklistedUser() {
    // Prepare test data with blacklisted user
    String userId = "urn:li:corpuser:blacklisted"; // This is in the userFilters
    String entityUrn = "urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)";
    String entityType = "dataset";
    String aspectName = "aspect1";

    // Create update aspect event
    EventData updateEvent =
        createUpdateAspectEvent(userId, entityUrn, entityType, aspectName, null, null, null);

    // Create a SpanData containing the event
    SpanData spanData = mock(SpanData.class);
    when(spanData.getEvents()).thenReturn(Collections.singletonList(updateEvent));

    // Export the span
    CompletableResultCode result = exporter.export(Collections.singletonList(spanData));

    // Verify result is successful
    assertTrue(result.isSuccess());

    // Verify no event was published due to user filter
    verify(mockProducer, never()).send(any(ProducerRecord.class));
  }

  @Test
  public void testFilterNonConfiguredAspect() {
    // Prepare test data
    String userId = "urn:li:corpuser:testUser";
    String entityUrn = "urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)";
    String entityType = "dataset";
    String aspectName = "nonConfiguredAspect"; // This is NOT in config.aspectTypes

    // Create update aspect event
    EventData updateEvent =
        createUpdateAspectEvent(userId, entityUrn, entityType, aspectName, null, null, null);

    // Create a SpanData containing the event
    SpanData spanData = mock(SpanData.class);
    when(spanData.getEvents()).thenReturn(Collections.singletonList(updateEvent));

    // Export the span
    CompletableResultCode result = exporter.export(Collections.singletonList(spanData));

    // Verify result is successful
    assertTrue(result.isSuccess());

    // Verify no event was published due to aspect filter
    verify(mockProducer, never()).send(any(ProducerRecord.class));
  }

  @Test
  public void testExportDeleteEntityEvent() {
    // Prepare test data
    String userId = "urn:li:corpuser:testUser";
    String entityUrn = "urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)";
    String entityType = "dataset";
    String aspectName = "aspect2"; // This is in the config

    // Create delete entity event
    EventData deleteEvent =
        createGenericEvent(
            UPDATE_ASPECT_EVENT,
            userId,
            DELETE_ENTITY_EVENT.getType(),
            entityUrn,
            entityType,
            aspectName,
            null,
            null,
            null);

    // Create a SpanData containing the event
    SpanData spanData = mock(SpanData.class);
    when(spanData.getEvents()).thenReturn(Collections.singletonList(deleteEvent));

    // Export the span
    CompletableResultCode result = exporter.export(Collections.singletonList(spanData));

    // Verify result is successful
    assertTrue(result.isSuccess());

    // Verify event was published
    verify(mockProducer).send(any(ProducerRecord.class));
  }

  @Test
  public void testExportMultipleEvents() {
    // Prepare test data
    String userId1 = "urn:li:corpuser:testUser1";
    String userId2 = "urn:li:corpuser:testUser2";

    // Create events
    EventData loginEvent = createLoginEvent(userId1, null, null, null, null);
    EventData updateEvent =
        createUpdateAspectEvent(
            userId2, "urn:li:dataset:123", "dataset", "aspect1", null, null, null);
    EventData nonMatchingEvent =
        createGenericEvent(
            "NON_MATCHING_EVENT", userId1, "RANDOM_EVENT", null, null, null, null, null, null);

    // Create SpanData objects
    SpanData spanData1 = mock(SpanData.class);
    when(spanData1.getEvents()).thenReturn(Collections.singletonList(loginEvent));

    SpanData spanData2 = mock(SpanData.class);
    when(spanData2.getEvents()).thenReturn(Arrays.asList(updateEvent, nonMatchingEvent));

    // Export spans
    CompletableResultCode result = exporter.export(Arrays.asList(spanData1, spanData2));

    // Verify result is successful
    assertTrue(result.isSuccess());

    // Verify two events were published (login and update, but not the non-matching one)
    verify(mockProducer, times(2)).send(any(ProducerRecord.class));
  }

  @Test
  public void testFlush() {
    CompletableResultCode result = exporter.flush();
    assertTrue(result.isSuccess());
    verify(mockProducer).flush();
  }

  @Test
  public void testShutdown() {
    CompletableResultCode result = exporter.shutdown();
    assertTrue(result.isSuccess());
    verify(mockProducer).flush();
  }

  // Helper methods to create test events
  private EventData createLoginEvent(
      String userId, String loginSource, String userAgent, String sourceIp, String traceId) {
    return createGenericEvent(
        LOGIN_EVENT,
        userId,
        DataHubUsageEventType.LOG_IN_EVENT.getType(),
        null,
        null,
        null,
        loginSource,
        userAgent,
        sourceIp,
        traceId);
  }

  private EventData createUpdateAspectEvent(
      String userId,
      String entityUrn,
      String entityType,
      String aspectName,
      String eventSource,
      String userAgent,
      String sourceIp) {
    return createGenericEvent(
        UPDATE_ASPECT_EVENT,
        userId,
        DataHubUsageEventType.UPDATE_ASPECT_EVENT.getType(),
        entityUrn,
        entityType,
        aspectName,
        null,
        userAgent,
        sourceIp,
        null,
        eventSource);
  }

  private EventData createGenericEvent(
      String eventName,
      String userId,
      String eventType,
      String entityUrn,
      String entityType,
      String aspectName,
      String loginSource,
      String userAgent,
      String sourceIp) {
    return createGenericEvent(
        eventName,
        userId,
        eventType,
        entityUrn,
        entityType,
        aspectName,
        loginSource,
        userAgent,
        sourceIp,
        null);
  }

  private EventData createGenericEvent(
      String eventName,
      String userId,
      String eventType,
      String entityUrn,
      String entityType,
      String aspectName,
      String loginSource,
      String userAgent,
      String sourceIp,
      String traceId) {
    return createGenericEvent(
        eventName,
        userId,
        eventType,
        entityUrn,
        entityType,
        aspectName,
        loginSource,
        userAgent,
        sourceIp,
        traceId,
        null);
  }

  private EventData createGenericEvent(
      String eventName,
      String userId,
      String eventType,
      String entityUrn,
      String entityType,
      String aspectName,
      String loginSource,
      String userAgent,
      String sourceIp,
      String traceId,
      String eventSource) {

    AttributesBuilder attributesBuilder = Attributes.builder();

    if (userId != null) {
      attributesBuilder.put(AttributeKey.stringKey(USER_ID_ATTR), userId);
    }

    if (eventType != null) {
      attributesBuilder.put(AttributeKey.stringKey(EVENT_TYPE_ATTR), eventType);
    }

    if (entityUrn != null) {
      attributesBuilder.put(AttributeKey.stringKey(ENTITY_URN_ATTR), entityUrn);
    }

    if (entityType != null) {
      attributesBuilder.put(AttributeKey.stringKey(ENTITY_TYPE_ATTR), entityType);
    }

    if (aspectName != null) {
      attributesBuilder.put(AttributeKey.stringKey(ASPECT_NAME_ATTR), aspectName);
    }

    if (loginSource != null) {
      attributesBuilder.put(AttributeKey.stringKey(LOGIN_SOURCE_ATTR), loginSource);
    }

    if (userAgent != null) {
      attributesBuilder.put(AttributeKey.stringKey(USER_AGENT_ATTR), userAgent);
    }

    if (sourceIp != null) {
      attributesBuilder.put(AttributeKey.stringKey(OpenTelemetryKeyConstants.SOURCE_IP), sourceIp);
    }

    if (traceId != null) {
      attributesBuilder.put(AttributeKey.stringKey(TELEMETRY_TRACE_ID_ATTR), traceId);
    }

    if (eventSource != null) {
      attributesBuilder.put(
          AttributeKey.stringKey(OpenTelemetryKeyConstants.EVENT_SOURCE), eventSource);
    }

    // Create event with attributes and timestamp
    Attributes attributes = attributesBuilder.build();
    EventData eventData = mock(EventData.class);
    when(eventData.getName()).thenReturn(eventName);
    when(eventData.getAttributes()).thenReturn(attributes);
    when(eventData.getEpochNanos()).thenReturn(System.nanoTime());

    return eventData;
  }
}
