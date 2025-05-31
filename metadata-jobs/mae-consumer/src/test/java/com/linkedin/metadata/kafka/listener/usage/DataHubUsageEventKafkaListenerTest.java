package com.linkedin.metadata.kafka.listener.usage;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.config.aws.AuditEventExportConfiguration;
import com.linkedin.metadata.datahubusage.DataHubUsageEventConstants;
import com.linkedin.mxe.SystemMetadata;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataHubUsageEventKafkaListenerTest {
  private DataHubUsageEventKafkaListener _listener;
  private AuditEventExportConfiguration _mockConfig;
  private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @BeforeMethod
  public void setup() {
    _mockConfig = mock(AuditEventExportConfiguration.class);
  }

  @Test
  public void testConstructorWithEmptyConfiguration() {
    // Test with empty/null configuration values
    when(_mockConfig.getUsageEventTypes()).thenReturn("");
    when(_mockConfig.getAspectTypes()).thenReturn("");
    when(_mockConfig.getUserFilters()).thenReturn("");

    _listener = new DataHubUsageEventKafkaListener(OBJECT_MAPPER, _mockConfig);

    assertNotNull(_listener);
  }

  @Test
  public void testConstructorWithNullConfiguration() {
    // Test with null configuration values
    when(_mockConfig.getUsageEventTypes()).thenReturn(null);
    when(_mockConfig.getAspectTypes()).thenReturn(null);
    when(_mockConfig.getUserFilters()).thenReturn(null);

    _listener = new DataHubUsageEventKafkaListener(OBJECT_MAPPER, _mockConfig);

    assertNotNull(_listener);
  }

  @Test
  public void testConstructorWithValidConfiguration() {
    // Test with valid configuration values
    when(_mockConfig.getUsageEventTypes()).thenReturn("LogInEvent,LogOutEvent,CreateUserEvent");
    when(_mockConfig.getAspectTypes()).thenReturn("DataHubPolicyInfo,CorpUserInfo");
    when(_mockConfig.getUserFilters()).thenReturn("urn:li:corpuser:admin,urn:li:corpuser:system");

    _listener = new DataHubUsageEventKafkaListener(OBJECT_MAPPER, _mockConfig);

    assertNotNull(_listener);
  }

  @Test
  public void testConvertRecordWithValidJson() throws IOException {
    setupListenerWithDefaultConfig();

    String validJson = "{\"type\":\"LogInEvent\",\"actorUrn\":\"urn:li:corpuser:test\"}";
    JsonNode result = _listener.convertRecord(validJson);

    assertNotNull(result);
    assertEquals(result.get("type").asText(), "LogInEvent");
    assertEquals(result.get("actorUrn").asText(), "urn:li:corpuser:test");
  }

  @Test(expectedExceptions = IOException.class)
  public void testConvertRecordWithInvalidJson() throws IOException {
    setupListenerWithDefaultConfig();

    String invalidJson = "invalid json string";
    _listener.convertRecord(invalidJson);
  }

  @Test
  public void testShouldSkipProcessingWithNullUsageSource() {
    setupListenerWithDefaultConfig();

    ObjectNode event = OBJECT_MAPPER.createObjectNode();
    event.put("type", "LogInEvent");
    // Missing usage source

    boolean result = _listener.shouldSkipProcessing(event);
    assertTrue(result, "Should skip processing when usage source is null");
  }

  @Test
  public void testShouldSkipProcessingWithWrongUsageSource() {
    setupListenerWithDefaultConfig();

    ObjectNode event = OBJECT_MAPPER.createObjectNode();
    event.put("type", "LogInEvent");
    event.put(DataHubUsageEventConstants.USAGE_SOURCE, "frontend");

    boolean result = _listener.shouldSkipProcessing(event);
    assertTrue(result, "Should skip processing when usage source is not 'backend'");
  }

  @Test
  public void testShouldSkipProcessingWithFilteredUser() {
    when(_mockConfig.getUsageEventTypes()).thenReturn("LogInEvent");
    when(_mockConfig.getAspectTypes()).thenReturn("");
    when(_mockConfig.getUserFilters()).thenReturn("urn:li:corpuser:admin,urn:li:corpuser:system");

    _listener = new DataHubUsageEventKafkaListener(OBJECT_MAPPER, _mockConfig);

    ObjectNode event = OBJECT_MAPPER.createObjectNode();
    event.put("type", "LogInEvent");
    event.put(DataHubUsageEventConstants.USAGE_SOURCE, DataHubUsageEventConstants.BACKEND_SOURCE);
    event.put(DataHubUsageEventConstants.ACTOR_URN, "urn:li:corpuser:admin");

    boolean result = _listener.shouldSkipProcessing(event);
    assertTrue(result, "Should skip processing for filtered users");
  }

  @Test
  public void testShouldSkipProcessingWithNullType() {
    setupListenerWithDefaultConfig();

    ObjectNode event = OBJECT_MAPPER.createObjectNode();
    event.put(DataHubUsageEventConstants.USAGE_SOURCE, DataHubUsageEventConstants.BACKEND_SOURCE);
    event.put(DataHubUsageEventConstants.ACTOR_URN, "urn:li:corpuser:test");
    // Missing type

    boolean result = _listener.shouldSkipProcessing(event);
    assertTrue(result, "Should skip processing when type is null");
  }

  @Test
  public void testShouldNotSkipProcessingWithMatchingEventType() {
    when(_mockConfig.getUsageEventTypes()).thenReturn("LogInEvent,LogOutEvent");
    when(_mockConfig.getAspectTypes()).thenReturn("");
    when(_mockConfig.getUserFilters()).thenReturn("");

    _listener = new DataHubUsageEventKafkaListener(OBJECT_MAPPER, _mockConfig);

    ObjectNode event = OBJECT_MAPPER.createObjectNode();
    event.put("type", "LogInEvent");
    event.put(DataHubUsageEventConstants.USAGE_SOURCE, DataHubUsageEventConstants.BACKEND_SOURCE);
    event.put(DataHubUsageEventConstants.ACTOR_URN, "urn:li:corpuser:test");

    boolean result = _listener.shouldSkipProcessing(event);
    assertFalse(result, "Should not skip processing when event type matches");
  }

  @Test
  public void testShouldNotSkipProcessingWithMatchingAspectType() {
    when(_mockConfig.getUsageEventTypes()).thenReturn("");
    when(_mockConfig.getAspectTypes()).thenReturn("DataHubPolicyInfo");
    when(_mockConfig.getUserFilters()).thenReturn("");

    _listener = new DataHubUsageEventKafkaListener(OBJECT_MAPPER, _mockConfig);

    ObjectNode event = OBJECT_MAPPER.createObjectNode();
    event.put("type", "UnknownEvent");
    event.put(DataHubUsageEventConstants.USAGE_SOURCE, DataHubUsageEventConstants.BACKEND_SOURCE);
    event.put(DataHubUsageEventConstants.ACTOR_URN, "urn:li:corpuser:test");
    event.put(DataHubUsageEventConstants.ASPECT_NAME, "DataHubPolicyInfo");

    boolean result = _listener.shouldSkipProcessing(event);
    assertFalse(result, "Should not skip processing when aspect type matches");
  }

  @Test
  public void testShouldSkipProcessingWithNoMatches() {
    when(_mockConfig.getUsageEventTypes()).thenReturn("LogInEvent");
    when(_mockConfig.getAspectTypes()).thenReturn("DataHubPolicyInfo");
    when(_mockConfig.getUserFilters()).thenReturn("");

    _listener = new DataHubUsageEventKafkaListener(OBJECT_MAPPER, _mockConfig);

    ObjectNode event = OBJECT_MAPPER.createObjectNode();
    event.put("type", "UnknownEvent");
    event.put(DataHubUsageEventConstants.USAGE_SOURCE, DataHubUsageEventConstants.BACKEND_SOURCE);
    event.put(DataHubUsageEventConstants.ACTOR_URN, "urn:li:corpuser:test");
    event.put(DataHubUsageEventConstants.ASPECT_NAME, "UnknownAspect");

    boolean result = _listener.shouldSkipProcessing(event);
    assertTrue(result, "Should skip processing when neither event type nor aspect type matches");
  }

  @Test
  public void testShouldNotSkipProcessingWithEitherMatch() {
    when(_mockConfig.getUsageEventTypes()).thenReturn("LogInEvent");
    when(_mockConfig.getAspectTypes()).thenReturn("DataHubPolicyInfo");
    when(_mockConfig.getUserFilters()).thenReturn("");

    _listener = new DataHubUsageEventKafkaListener(OBJECT_MAPPER, _mockConfig);

    ObjectNode event = OBJECT_MAPPER.createObjectNode();
    event.put("type", "LogInEvent");
    event.put(DataHubUsageEventConstants.USAGE_SOURCE, DataHubUsageEventConstants.BACKEND_SOURCE);
    event.put(DataHubUsageEventConstants.ACTOR_URN, "urn:li:corpuser:test");
    event.put(DataHubUsageEventConstants.ASPECT_NAME, "UnknownAspect");

    boolean result = _listener.shouldSkipProcessing(event);
    assertFalse(
        result, "Should not skip processing when event type matches even if aspect doesn't");
  }

  @Test
  public void testShouldSkipProcessingWithNullAspectName() {
    when(_mockConfig.getUsageEventTypes()).thenReturn("");
    when(_mockConfig.getAspectTypes()).thenReturn("DataHubPolicyInfo");
    when(_mockConfig.getUserFilters()).thenReturn("");

    _listener = new DataHubUsageEventKafkaListener(OBJECT_MAPPER, _mockConfig);

    ObjectNode event = OBJECT_MAPPER.createObjectNode();
    event.put("type", "UnknownEvent");
    event.put(DataHubUsageEventConstants.USAGE_SOURCE, DataHubUsageEventConstants.BACKEND_SOURCE);
    event.put(DataHubUsageEventConstants.ACTOR_URN, "urn:li:corpuser:test");
    // Missing aspect name

    boolean result = _listener.shouldSkipProcessing(event);
    assertTrue(
        result, "Should skip processing when aspect name is null and event type doesn't match");
  }

  @Test
  public void testGetFineGrainedLoggingAttributes() {
    setupListenerWithDefaultConfig();

    ObjectNode event = OBJECT_MAPPER.createObjectNode();
    List<String> result = _listener.getFineGrainedLoggingAttributes(event);

    assertNotNull(result);
    assertEquals(result, Collections.emptyList());
  }

  @Test
  public void testGetSystemMetadata() {
    setupListenerWithDefaultConfig();

    ObjectNode event = OBJECT_MAPPER.createObjectNode();
    SystemMetadata result = _listener.getSystemMetadata(event);

    assertNotNull(result);
    // Verify it returns a default SystemMetadata (we can't easily test the exact content
    // without knowing the implementation details of
    // SystemMetadataUtils.createDefaultSystemMetadata())
  }

  @Test
  public void testGetEventDisplayString() {
    setupListenerWithDefaultConfig();

    ObjectNode event = OBJECT_MAPPER.createObjectNode();
    event.put("type", "LogInEvent");
    event.put("actorUrn", "urn:li:corpuser:test");

    String result = _listener.getEventDisplayString(event);

    assertNotNull(result);
    assertTrue(result.contains("LogInEvent"));
    assertTrue(result.contains("urn:li:corpuser:test"));
  }

  @Test
  public void testSetMDCContext() {
    setupListenerWithDefaultConfig();

    ObjectNode event = OBJECT_MAPPER.createObjectNode();
    // This is a no-op method, just verify it doesn't throw an exception
    _listener.setMDCContext(event);
  }

  @Test
  public void testCompleteWorkflowWithValidEvent() {
    when(_mockConfig.getUsageEventTypes()).thenReturn("LogInEvent,LogOutEvent,CreateUserEvent");
    when(_mockConfig.getAspectTypes()).thenReturn("DataHubPolicyInfo");
    when(_mockConfig.getUserFilters()).thenReturn("urn:li:corpuser:admin");

    _listener = new DataHubUsageEventKafkaListener(OBJECT_MAPPER, _mockConfig);

    ObjectNode event = OBJECT_MAPPER.createObjectNode();
    event.put("type", "LogInEvent");
    event.put(DataHubUsageEventConstants.USAGE_SOURCE, DataHubUsageEventConstants.BACKEND_SOURCE);
    event.put(DataHubUsageEventConstants.ACTOR_URN, "urn:li:corpuser:test");
    event.put("timestamp", System.currentTimeMillis());

    // Test the complete workflow
    boolean shouldSkip = _listener.shouldSkipProcessing(event);
    assertFalse(shouldSkip, "Should not skip processing for valid matching event");

    List<String> loggingAttributes = _listener.getFineGrainedLoggingAttributes(event);
    assertEquals(loggingAttributes, Collections.emptyList());

    SystemMetadata systemMetadata = _listener.getSystemMetadata(event);
    assertNotNull(systemMetadata);

    String displayString = _listener.getEventDisplayString(event);
    assertTrue(displayString.contains("LogInEvent"));
  }

  private void setupListenerWithDefaultConfig() {
    when(_mockConfig.getUsageEventTypes()).thenReturn("LogInEvent,LogOutEvent");
    when(_mockConfig.getAspectTypes()).thenReturn("DataHubPolicyInfo");
    when(_mockConfig.getUserFilters()).thenReturn("");

    _listener = new DataHubUsageEventKafkaListener(OBJECT_MAPPER, _mockConfig);
  }
}
