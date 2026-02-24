package com.linkedin.metadata.kafka.transformer;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.datahubusage.DataHubUsageEventType;
import com.linkedin.metadata.kafka.hydrator.EntityHydrator;
import com.linkedin.metadata.kafka.hydrator.EntityType;
import java.time.Instant;
import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataHubUsageEventTransformerTest {
  private DataHubUsageEventTransformer _transformer;
  private EntityHydrator _mockEntityHydrator;
  private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @BeforeMethod
  public void setup() {
    _mockEntityHydrator = mock(EntityHydrator.class);
    _transformer = new DataHubUsageEventTransformer(_mockEntityHydrator);
  }

  @Test
  public void testTransformDataHubUsageEventWithInvalidJson() {
    // Test with invalid JSON
    Optional<DataHubUsageEventTransformer.TransformedDocument> result =
        _transformer.transformDataHubUsageEvent("invalid json");

    // Should return empty optional when JSON is invalid
    assertFalse(result.isPresent());
  }

  @Test
  public void testTransformDataHubUsageEventWithMissingType() {
    // Create a usage event without type field
    ObjectNode usageEvent = OBJECT_MAPPER.createObjectNode();
    usageEvent.put("timestamp", System.currentTimeMillis());
    usageEvent.put("actorUrn", "urn:li:corpuser:testUser");

    Optional<DataHubUsageEventTransformer.TransformedDocument> result =
        _transformer.transformDataHubUsageEvent(usageEvent.toString());

    // Should return empty optional when type is missing
    assertFalse(result.isPresent());
  }

  @Test
  public void testTransformDataHubUsageEventWithInvalidType() {
    // Create a usage event with invalid type
    ObjectNode usageEvent = OBJECT_MAPPER.createObjectNode();
    usageEvent.put("type", "INVALID_TYPE");
    usageEvent.put("timestamp", System.currentTimeMillis());
    usageEvent.put("actorUrn", "urn:li:corpuser:testUser");

    Optional<DataHubUsageEventTransformer.TransformedDocument> result =
        _transformer.transformDataHubUsageEvent(usageEvent.toString());

    // Should return empty optional when type is invalid
    assertFalse(result.isPresent());
  }

  @Test
  public void testTransformDataHubUsageEventWithNumericTimestamp() throws Exception {
    // Create a usage event with numeric timestamp
    long timestamp = System.currentTimeMillis();
    String actorUrn = "urn:li:corpuser:testUser";

    ObjectNode usageEvent = OBJECT_MAPPER.createObjectNode();
    usageEvent.put("type", DataHubUsageEventType.LOG_IN_EVENT.getType());
    usageEvent.put("timestamp", timestamp);
    usageEvent.put("actorUrn", actorUrn);

    // Mock entity hydrator
    ObjectNode hydratedEntity = OBJECT_MAPPER.createObjectNode();
    hydratedEntity.put("username", "testUser");
    hydratedEntity.put("fullName", "Test User");

    when(_mockEntityHydrator.getHydratedEntity(actorUrn)).thenReturn(Optional.of(hydratedEntity));

    // Transform the event
    Optional<DataHubUsageEventTransformer.TransformedDocument> result =
        _transformer.transformDataHubUsageEvent(usageEvent.toString());

    // Verify result
    assertTrue(result.isPresent());
    DataHubUsageEventTransformer.TransformedDocument doc = result.get();

    // Verify ID format
    assertEquals(
        doc.getId(),
        DataHubUsageEventType.LOG_IN_EVENT.getType() + "_" + actorUrn + "_" + timestamp);

    // Parse the document to verify fields
    ObjectNode transformedDoc = (ObjectNode) OBJECT_MAPPER.readTree(doc.getDocument());
    assertEquals(transformedDoc.get("type").asText(), DataHubUsageEventType.LOG_IN_EVENT.getType());
    assertEquals(transformedDoc.get("timestamp").asLong(), timestamp);
    assertEquals(transformedDoc.get("@timestamp").asLong(), timestamp);
    assertEquals(transformedDoc.get("actorUrn").asText(), actorUrn);

    // Verify hydrated actor fields
    assertEquals(transformedDoc.get("corp_user_username").asText(), "testUser");
    assertEquals(transformedDoc.get("corp_user_fullName").asText(), "Test User");

    // Verify the entity hydrator was called correctly
    verify(_mockEntityHydrator).getHydratedEntity(actorUrn);
  }

  @Test
  public void testTransformDataHubUsageEventWithISOTimestamp() throws Exception {
    // Create a usage event with ISO string timestamp
    String isoTimestamp = Instant.now().toString();
    long expectedTimestamp = Instant.parse(isoTimestamp).toEpochMilli();
    String actorUrn = "urn:li:corpuser:testUser";

    ObjectNode usageEvent = OBJECT_MAPPER.createObjectNode();
    usageEvent.put("type", DataHubUsageEventType.LOG_IN_EVENT.getType());
    usageEvent.put("timestamp", isoTimestamp);
    usageEvent.put("actorUrn", actorUrn);

    // Mock entity hydrator
    ObjectNode hydratedEntity = OBJECT_MAPPER.createObjectNode();
    hydratedEntity.put("username", "testUser");

    when(_mockEntityHydrator.getHydratedEntity(actorUrn)).thenReturn(Optional.of(hydratedEntity));

    // Transform the event
    Optional<DataHubUsageEventTransformer.TransformedDocument> result =
        _transformer.transformDataHubUsageEvent(usageEvent.toString());

    // Verify result
    assertTrue(result.isPresent());

    // Parse the document to verify fields
    ObjectNode transformedDoc = (ObjectNode) OBJECT_MAPPER.readTree(result.get().getDocument());
    assertEquals(transformedDoc.get("timestamp").asLong(), expectedTimestamp);
    assertEquals(transformedDoc.get("@timestamp").asLong(), expectedTimestamp);
  }

  @Test
  public void testTransformDataHubUsageEventWithInvalidTimestamp() throws Exception {
    // Create a usage event with invalid timestamp format
    String actorUrn = "urn:li:corpuser:testUser";

    ObjectNode usageEvent = OBJECT_MAPPER.createObjectNode();
    usageEvent.put("type", DataHubUsageEventType.LOG_IN_EVENT.getType());
    usageEvent.put("timestamp", "invalid-timestamp");
    usageEvent.put("actorUrn", actorUrn);

    // Mock entity hydrator
    ObjectNode hydratedEntity = OBJECT_MAPPER.createObjectNode();
    hydratedEntity.put("username", "testUser");

    when(_mockEntityHydrator.getHydratedEntity(actorUrn)).thenReturn(Optional.of(hydratedEntity));

    // Capture current time for comparison
    long beforeTransform = Instant.now().toEpochMilli();

    // Transform the event
    Optional<DataHubUsageEventTransformer.TransformedDocument> result =
        _transformer.transformDataHubUsageEvent(usageEvent.toString());

    long afterTransform = Instant.now().toEpochMilli();

    // Verify result
    assertTrue(result.isPresent());

    // Parse the document to verify fields - should use current time as fallback
    ObjectNode transformedDoc = (ObjectNode) OBJECT_MAPPER.readTree(result.get().getDocument());
    long timestamp = transformedDoc.get("timestamp").asLong();
    assertTrue(
        timestamp >= beforeTransform && timestamp <= afterTransform,
        "Expected timestamp to be between "
            + beforeTransform
            + " and "
            + afterTransform
            + " but was "
            + timestamp);
  }

  @Test
  public void testTransformDataHubUsageEventWithEntityUrn() throws Exception {
    // Test event types that include an entity URN
    String actorUrn = "urn:li:corpuser:testUser";
    String entityUrn = "urn:li:dataset:(urn:li:dataPlatform:hive,testTable,PROD)";
    String entityType = EntityType.DATASET.name();

    ObjectNode usageEvent = OBJECT_MAPPER.createObjectNode();
    usageEvent.put("type", DataHubUsageEventType.ENTITY_VIEW_EVENT.getType());
    usageEvent.put("timestamp", System.currentTimeMillis());
    usageEvent.put("actorUrn", actorUrn);
    usageEvent.put("entityUrn", entityUrn);
    usageEvent.put("entityType", entityType);

    // Mock entity hydrators
    ObjectNode hydratedActor = OBJECT_MAPPER.createObjectNode();
    hydratedActor.put("username", "testUser");

    ObjectNode hydratedEntity = OBJECT_MAPPER.createObjectNode();
    hydratedEntity.put("name", "testTable");
    hydratedEntity.put("platform", "hive");

    when(_mockEntityHydrator.getHydratedEntity(actorUrn)).thenReturn(Optional.of(hydratedActor));
    when(_mockEntityHydrator.getHydratedEntity(entityUrn)).thenReturn(Optional.of(hydratedEntity));

    // Transform the event
    Optional<DataHubUsageEventTransformer.TransformedDocument> result =
        _transformer.transformDataHubUsageEvent(usageEvent.toString());

    // Verify result
    assertTrue(result.isPresent());

    // Parse the document to verify fields
    ObjectNode transformedDoc = (ObjectNode) OBJECT_MAPPER.readTree(result.get().getDocument());

    // Verify hydrated entity fields
    assertEquals(transformedDoc.get("dataset_name").asText(), "testTable");
    assertEquals(transformedDoc.get("dataset_platform").asText(), "hive");

    // Verify the entity hydrator was called for both actor and entity
    verify(_mockEntityHydrator).getHydratedEntity(actorUrn);
    verify(_mockEntityHydrator).getHydratedEntity(entityUrn);
  }

  @Test
  public void testTransformDataHubUsageEventWithEntityTypeOnlyEntityUrn() throws Exception {
    // Test event types that include an entity URN
    String actorUrn = "urn:li:corpuser:testUser";
    String entityUrn = "urn:li:dataset";
    String entityType = EntityType.DATASET.name();

    ObjectNode usageEvent = OBJECT_MAPPER.createObjectNode();
    usageEvent.put("type", DataHubUsageEventType.ENTITY_VIEW_EVENT.getType());
    usageEvent.put("timestamp", System.currentTimeMillis());
    usageEvent.put("actorUrn", actorUrn);
    usageEvent.put("entityUrn", entityUrn);
    usageEvent.put("entityType", entityType);

    // Mock entity hydrators
    ObjectNode hydratedActor = OBJECT_MAPPER.createObjectNode();
    hydratedActor.put("username", "testUser");

    when(_mockEntityHydrator.getHydratedEntity(actorUrn)).thenReturn(Optional.of(hydratedActor));
    when(_mockEntityHydrator.getHydratedEntity(entityUrn)).thenReturn(Optional.empty());

    // Transform the event
    Optional<DataHubUsageEventTransformer.TransformedDocument> result =
        _transformer.transformDataHubUsageEvent(usageEvent.toString());

    // Verify result
    assertTrue(result.isPresent());

    // Parse the document to verify fields
    ObjectNode transformedDoc = (ObjectNode) OBJECT_MAPPER.readTree(result.get().getDocument());

    // Verify hydrated entity fields
    assertFalse(transformedDoc.has("dataset_name"));
    assertFalse(transformedDoc.has("dataset_platform"));

    // Verify the entity hydrator was called for both actor and entity
    verify(_mockEntityHydrator).getHydratedEntity(actorUrn);
    verify(_mockEntityHydrator).getHydratedEntity(entityUrn);
  }

  @Test
  public void testTransformDataHubUsageEventWithNonExistentEntity() throws Exception {
    // Test with an entity that doesn't exist in the system
    String actorUrn = "urn:li:corpuser:testUser";
    String entityUrn = "urn:li:dataset:(urn:li:dataPlatform:hive,nonExistentTable,PROD)";
    String entityType = EntityType.DATASET.name();

    ObjectNode usageEvent = OBJECT_MAPPER.createObjectNode();
    usageEvent.put("type", DataHubUsageEventType.ENTITY_VIEW_EVENT.getType());
    usageEvent.put("timestamp", System.currentTimeMillis());
    usageEvent.put("actorUrn", actorUrn);
    usageEvent.put("entityUrn", entityUrn);
    usageEvent.put("entityType", entityType);

    // Mock entity hydrator to return empty for entity
    ObjectNode hydratedActor = OBJECT_MAPPER.createObjectNode();
    hydratedActor.put("username", "testUser");

    when(_mockEntityHydrator.getHydratedEntity(actorUrn)).thenReturn(Optional.of(hydratedActor));
    when(_mockEntityHydrator.getHydratedEntity(entityUrn)).thenReturn(Optional.empty());

    // Transform the event
    Optional<DataHubUsageEventTransformer.TransformedDocument> result =
        _transformer.transformDataHubUsageEvent(usageEvent.toString());

    // Verify result - should still produce a document even if entity doesn't exist
    assertTrue(result.isPresent());

    // Parse the document to verify fields
    ObjectNode transformedDoc = (ObjectNode) OBJECT_MAPPER.readTree(result.get().getDocument());

    // Verify no entity fields were added
    assertFalse(transformedDoc.has("dataset_name"));

    // Verify the entity hydrator was called for both
    verify(_mockEntityHydrator).getHydratedEntity(actorUrn);
    verify(_mockEntityHydrator).getHydratedEntity(entityUrn);
  }

  @Test
  public void testTransformDataHubUsageEventWithUnsupportedEntityType() throws Exception {
    // Test with an unsupported entity type
    String actorUrn = "urn:li:corpuser:testUser";
    String entityUrn = "urn:li:something:123";

    ObjectNode usageEvent = OBJECT_MAPPER.createObjectNode();
    usageEvent.put("type", DataHubUsageEventType.ENTITY_VIEW_EVENT.getType());
    usageEvent.put("timestamp", System.currentTimeMillis());
    usageEvent.put("actorUrn", actorUrn);
    usageEvent.put("entityUrn", entityUrn);
    usageEvent.put("entityType", "UNSUPPORTED_TYPE");

    // Mock entity hydrator
    ObjectNode hydratedActor = OBJECT_MAPPER.createObjectNode();
    hydratedActor.put("username", "testUser");

    when(_mockEntityHydrator.getHydratedEntity(actorUrn)).thenReturn(Optional.of(hydratedActor));

    // Transform the event
    Optional<DataHubUsageEventTransformer.TransformedDocument> result =
        _transformer.transformDataHubUsageEvent(usageEvent.toString());

    // Verify result - should still produce a document
    assertTrue(result.isPresent());

    // No call to hydrate the entity with unsupported type
    verify(_mockEntityHydrator, never()).getHydratedEntity(entityUrn);
  }

  @Test
  public void testTransformDataHubUsageEventWithoutEntityFields() throws Exception {
    // Test with event that should have entity fields but they're missing
    String actorUrn = "urn:li:corpuser:testUser";

    ObjectNode usageEvent = OBJECT_MAPPER.createObjectNode();
    usageEvent.put("type", DataHubUsageEventType.ENTITY_VIEW_EVENT.getType());
    usageEvent.put("timestamp", System.currentTimeMillis());
    usageEvent.put("actorUrn", actorUrn);
    // Missing entityUrn and entityType

    // Mock entity hydrator
    ObjectNode hydratedActor = OBJECT_MAPPER.createObjectNode();
    hydratedActor.put("username", "testUser");

    when(_mockEntityHydrator.getHydratedEntity(actorUrn)).thenReturn(Optional.of(hydratedActor));

    // Transform the event
    Optional<DataHubUsageEventTransformer.TransformedDocument> result =
        _transformer.transformDataHubUsageEvent(usageEvent.toString());

    // Verify result - should still produce a document
    assertTrue(result.isPresent());

    // Parse the document to verify fields - should only have actor fields
    ObjectNode transformedDoc = (ObjectNode) OBJECT_MAPPER.readTree(result.get().getDocument());
    assertTrue(transformedDoc.has("corp_user_username"));

    // Verify only the actor hydrator was called
    verify(_mockEntityHydrator).getHydratedEntity(actorUrn);
    // No other hydrator calls
    verifyNoMoreInteractions(_mockEntityHydrator);
  }
}
