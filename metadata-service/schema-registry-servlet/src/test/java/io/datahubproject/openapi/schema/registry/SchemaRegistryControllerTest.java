package io.datahubproject.openapi.schema.registry;

import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.EventSchemaConstants;
import com.linkedin.metadata.EventSchemaData;
import com.linkedin.metadata.SchemaConfigLoader;
import com.linkedin.metadata.SchemaIdOrdinal;
import com.linkedin.metadata.registry.SchemaRegistryService;
import com.linkedin.metadata.registry.SchemaRegistryServiceImpl;
import com.linkedin.mxe.TopicConvention;
import com.linkedin.mxe.TopicConventionImpl;
import com.linkedin.mxe.Topics;
import io.datahubproject.schema_registry.openapi.generated.Config;
import io.datahubproject.schema_registry.openapi.generated.Schema;
import java.util.List;
import java.util.Optional;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Consolidated test suite for SchemaRegistryController.
 *
 * <p>This test class combines: - Original mocked tests for basic functionality - Real
 * implementation tests for MCL schema validation - Ordinal-based schema ID functionality tests
 *
 * <p>The tests are organized into sections: 1. Basic functionality tests (mocked) 2. Real
 * implementation tests (using actual EventSchemaData and SchemaRegistryServiceImpl) 3. MCL-specific
 * tests (using real implementations) 4. Error handling tests
 */
public class SchemaRegistryControllerTest {

  // MCL Topic and Subject names for real implementation tests
  private static final String MCL_VERSIONED_TOPIC = Topics.METADATA_CHANGE_LOG_VERSIONED;
  private static final String MCL_VERSIONED_SUBJECT = MCL_VERSIONED_TOPIC + "-value";

  // Expected schema IDs based on SchemaIdOrdinal enum
  private static final int MCL_V1_SCHEMA_ID = 2; // METADATA_CHANGE_LOG_V1
  private static final int MCL_TIMESERIES_V1_SCHEMA_ID = 3; // METADATA_CHANGE_LOG_TIMESERIES_V1
  private static final int MCL_V1_FIX_SCHEMA_ID = 11; // METADATA_CHANGE_LOG_V1_FIX
  private static final int MCL_TIMESERIES_V1_FIX_SCHEMA_ID =
      12; // METADATA_CHANGE_LOG_TIMESERIES_V1_FIX
  private static final int MCL_NEW_SCHEMA_ID = 18; // METADATA_CHANGE_LOG
  private static final int MCL_TIMESERIES_NEW_SCHEMA_ID = 19; // METADATA_CHANGE_LOG_TIMESERIES

  // Mocked dependencies for basic tests
  @Mock private SchemaRegistryService mockSchemaRegistryService;
  @Mock private TopicConvention mockTopicConvention;

  private SchemaRegistryController controller;
  private MockHttpServletRequest mockRequest;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    mockRequest = new MockHttpServletRequest();
    controller =
        new SchemaRegistryController(new ObjectMapper(), mockRequest, mockSchemaRegistryService);
  }

  // ============================================================================
  // SECTION 1: BASIC FUNCTIONALITY TESTS (MOCKED)
  // ============================================================================

  @Test
  public void testGetSchemaTypes() {
    ResponseEntity<List<String>> response = controller.getSchemaTypes();
    assertEquals(response.getStatusCode(), HttpStatus.OK);
    assertNotNull(response.getBody());
    assertEquals(response.getBody().size(), 1);
    assertTrue(response.getBody().contains("AVRO"));
  }

  @Test
  public void testGetTopLevelConfig() {
    ResponseEntity<Config> response = controller.getTopLevelConfig();
    assertEquals(response.getStatusCode(), HttpStatus.OK);
    assertNotNull(response.getBody());
    assertEquals(
        response.getBody().getCompatibilityLevel(), Config.CompatibilityLevelEnum.BACKWARD);
  }

  @Test
  public void testListSubjects() {
    List<String> mockSubjects = List.of("test-subject-value", "another-subject-value");
    when(mockSchemaRegistryService.getAllTopics())
        .thenReturn(List.of("test-subject", "another-subject"));

    ResponseEntity<List<String>> response = controller.list(null, false, false);
    assertEquals(response.getStatusCode(), HttpStatus.OK);
    assertNotNull(response.getBody());
    assertEquals(response.getBody().size(), 2);
    assertTrue(response.getBody().contains("test-subject-value"));
    assertTrue(response.getBody().contains("another-subject-value"));
  }

  @Test
  public void testListVersions() {
    List<Integer> mockVersions = List.of(1, 2, 3);
    when(mockSchemaRegistryService.getSupportedSchemaVersionsForTopic("test-topic"))
        .thenReturn(Optional.of(mockVersions));

    ResponseEntity<List<Integer>> response =
        controller.listVersions("test-topic-value", false, false);
    assertEquals(response.getStatusCode(), HttpStatus.OK);
    assertNotNull(response.getBody());
    assertEquals(response.getBody(), mockVersions);
  }

  @Test
  public void testGetSchemaByVersion_Latest() {
    org.apache.avro.Schema mockSchema =
        org.apache.avro.Schema.createRecord("TestSchema", null, "test.namespace", false);
    when(mockSchemaRegistryService.getLatestSchemaVersionForTopic("test-topic"))
        .thenReturn(Optional.of(3));
    when(mockSchemaRegistryService.getSchemaIdForTopicAndVersion("test-topic", 3))
        .thenReturn(Optional.of(123));
    when(mockSchemaRegistryService.getSchemaForTopicAndVersion("test-topic", 3))
        .thenReturn(Optional.of(mockSchema));

    ResponseEntity<Schema> response =
        controller.getSchemaByVersion("test-topic-value", "latest", false);
    assertEquals(response.getStatusCode(), HttpStatus.OK);
    assertNotNull(response.getBody());
    assertEquals(response.getBody().getSubject(), "test-topic-value");
    assertEquals(response.getBody().getVersion(), 3);
    assertEquals(response.getBody().getId(), 123);
    assertEquals(response.getBody().getSchema(), mockSchema.toString());
  }

  @Test
  public void testGetSchemaByVersion_SpecificVersion() {
    org.apache.avro.Schema mockSchema =
        org.apache.avro.Schema.createRecord("TestSchema", null, "test.namespace", false);
    when(mockSchemaRegistryService.getSchemaIdForTopicAndVersion("test-topic", 2))
        .thenReturn(Optional.of(456));
    when(mockSchemaRegistryService.getSchemaForTopicAndVersion("test-topic", 2))
        .thenReturn(Optional.of(mockSchema));

    ResponseEntity<Schema> response = controller.getSchemaByVersion("test-topic-value", "2", false);
    assertEquals(response.getStatusCode(), HttpStatus.OK);
    assertNotNull(response.getBody());
    assertEquals(response.getBody().getSubject(), "test-topic-value");
    assertEquals(response.getBody().getVersion(), 2);
    assertEquals(response.getBody().getId(), 456);
    assertEquals(response.getBody().getSchema(), mockSchema.toString());
  }

  @Test
  public void testGetSchemaByVersion_NotFound() {
    when(mockSchemaRegistryService.getSchemaIdForTopicAndVersion("nonexistent-topic", 1))
        .thenReturn(Optional.empty());

    ResponseEntity<Schema> response =
        controller.getSchemaByVersion("nonexistent-topic-value", "1", false);
    assertEquals(response.getStatusCode(), HttpStatus.NOT_FOUND);
  }

  // ============================================================================
  // SECTION 2: REAL IMPLEMENTATION TESTS
  // ============================================================================

  @Test
  public void testRealImplementation_BasicFunctionality() {
    // Create real instances for comprehensive testing
    EventSchemaData eventSchemaData = new EventSchemaData();
    TopicConventionImpl topicConvention = new TopicConventionImpl();
    SchemaRegistryServiceImpl realService =
        new SchemaRegistryServiceImpl(topicConvention, eventSchemaData);

    SchemaRegistryController realController =
        new SchemaRegistryController(new ObjectMapper(), mockRequest, realService);

    // Test schema types endpoint
    ResponseEntity<List<String>> schemaTypesResponse = realController.getSchemaTypes();
    assertEquals(schemaTypesResponse.getStatusCode(), HttpStatus.OK);
    assertNotNull(schemaTypesResponse.getBody());
    assertEquals(schemaTypesResponse.getBody().size(), 1);
    assertTrue(schemaTypesResponse.getBody().contains("AVRO"));

    // Test top-level configuration
    ResponseEntity<Config> configResponse = realController.getTopLevelConfig();
    assertEquals(configResponse.getStatusCode(), HttpStatus.OK);
    assertNotNull(configResponse.getBody());
    assertEquals(
        configResponse.getBody().getCompatibilityLevel(), Config.CompatibilityLevelEnum.BACKWARD);
  }

  @Test
  public void testRealImplementation_ListAllSubjects() {
    // Create real instances
    EventSchemaData eventSchemaData = new EventSchemaData();
    TopicConventionImpl topicConvention = new TopicConventionImpl();
    SchemaRegistryServiceImpl realService =
        new SchemaRegistryServiceImpl(topicConvention, eventSchemaData);

    SchemaRegistryController realController =
        new SchemaRegistryController(new ObjectMapper(), mockRequest, realService);

    ResponseEntity<List<String>> response = realController.list(null, false, false);
    assertEquals(response.getStatusCode(), HttpStatus.OK);
    assertNotNull(response.getBody());

    // Should have some subjects
    assertTrue(response.getBody().size() > 0, "Should have at least some subjects");

    // Print all subjects for debugging
    System.out.println("Available subjects:");
    for (String subject : response.getBody()) {
      System.out.println("  - " + subject);
    }
  }

  // ============================================================================
  // SECTION 3: MCL-SPECIFIC TESTS (REAL IMPLEMENTATIONS)
  // ============================================================================

  @Test
  public void testMCLSchema_AllVersionsExist() {
    // Create real instances
    EventSchemaData eventSchemaData = new EventSchemaData();
    TopicConventionImpl topicConvention = new TopicConventionImpl();
    SchemaRegistryServiceImpl realService =
        new SchemaRegistryServiceImpl(topicConvention, eventSchemaData);

    SchemaRegistryController realController =
        new SchemaRegistryController(new ObjectMapper(), mockRequest, realService);

    // Test that MCL Versioned subject exists
    ResponseEntity<List<String>> allSubjectsResponse = realController.list(null, false, false);
    List<String> allSubjects = allSubjectsResponse.getBody();

    assertTrue(allSubjects.contains(MCL_VERSIONED_SUBJECT), "MCL Versioned subject should exist");

    // Test listing versions
    ResponseEntity<List<Integer>> versionsResponse =
        realController.listVersions(MCL_VERSIONED_SUBJECT, false, false);
    assertEquals(versionsResponse.getStatusCode(), HttpStatus.OK);
    assertNotNull(versionsResponse.getBody());

    List<Integer> versions = versionsResponse.getBody();
    assertEquals(versions.size(), 6, "MCL should have 6 versions");
    assertTrue(versions.contains(1), "Should have version 1");
    assertTrue(versions.contains(2), "Should have version 2");
    assertTrue(versions.contains(3), "Should have version 3");
    assertTrue(versions.contains(4), "Should have version 4");
    assertTrue(versions.contains(5), "Should have version 5");
    assertTrue(versions.contains(6), "Should have version 6");
  }

  @Test
  public void testMCLSchema_CorrectSchemaIds() {
    // Create real instances
    EventSchemaData eventSchemaData = new EventSchemaData();
    TopicConventionImpl topicConvention = new TopicConventionImpl();
    SchemaRegistryServiceImpl realService =
        new SchemaRegistryServiceImpl(topicConvention, eventSchemaData);

    SchemaRegistryController realController =
        new SchemaRegistryController(new ObjectMapper(), mockRequest, realService);

    // Test each version has the correct schema ID
    ResponseEntity<List<Integer>> versionsResponse =
        realController.listVersions(MCL_VERSIONED_SUBJECT, false, false);
    List<Integer> versions = versionsResponse.getBody();

    for (Integer version : versions) {
      ResponseEntity<Schema> versionResponse =
          realController.getSchemaByVersion(MCL_VERSIONED_SUBJECT, String.valueOf(version), false);
      assertEquals(versionResponse.getStatusCode(), HttpStatus.OK);

      Schema schema = versionResponse.getBody();
      int actualSchemaId = schema.getId();

      // Verify correct schema ID for each version
      switch (version) {
        case 1:
          assertEquals(actualSchemaId, MCL_V1_SCHEMA_ID, "Version 1 should have schema ID 2");
          break;
        case 2:
          assertEquals(
              actualSchemaId, MCL_TIMESERIES_V1_SCHEMA_ID, "Version 2 should have schema ID 3");
          break;
        case 3:
          assertEquals(actualSchemaId, MCL_V1_FIX_SCHEMA_ID, "Version 3 should have schema ID 11");
          break;
        case 4:
          assertEquals(
              actualSchemaId,
              MCL_TIMESERIES_V1_FIX_SCHEMA_ID,
              "Version 4 should have schema ID 12");
          break;
        case 5:
          assertEquals(actualSchemaId, MCL_NEW_SCHEMA_ID, "Version 5 should have schema ID 18");
          break;
        case 6:
          assertEquals(
              actualSchemaId, MCL_TIMESERIES_NEW_SCHEMA_ID, "Version 6 should have schema ID 19");
          break;
        default:
          fail("Unexpected version: " + version);
      }

      // Verify schema content exists
      assertNotNull(schema.getSchema(), "Schema content should not be null for version " + version);
      assertTrue(
          schema.getSchema().contains("record"),
          "Schema should contain 'record' for version " + version);
    }
  }

  @Test
  public void testMCLSchema_LatestVersion() {
    // Create real instances
    EventSchemaData eventSchemaData = new EventSchemaData();
    TopicConventionImpl topicConvention = new TopicConventionImpl();
    SchemaRegistryServiceImpl realService =
        new SchemaRegistryServiceImpl(topicConvention, eventSchemaData);

    SchemaRegistryController realController =
        new SchemaRegistryController(new ObjectMapper(), mockRequest, realService);

    // Test getting latest version
    ResponseEntity<Schema> latestResponse =
        realController.getSchemaByVersion(MCL_VERSIONED_SUBJECT, "latest", false);
    assertEquals(latestResponse.getStatusCode(), HttpStatus.OK);
    assertNotNull(latestResponse.getBody());

    Schema latestSchema = latestResponse.getBody();
    assertEquals(latestSchema.getVersion(), 6, "Latest version should be 6");
    assertEquals(
        latestSchema.getId(),
        MCL_TIMESERIES_NEW_SCHEMA_ID,
        "Latest version should have schema ID 19");
    assertNotNull(latestSchema.getSchema(), "Latest schema content should not be null");
  }

  // ============================================================================
  // SECTION 4: ERROR HANDLING TESTS
  // ============================================================================

  @Test
  public void testErrorHandling_NonExistentSubject() {
    ResponseEntity<Schema> response =
        controller.getSchemaByVersion("NonExistentSubject-value", "1", false);
    assertEquals(response.getStatusCode(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testErrorHandling_NonExistentVersion() {
    when(mockSchemaRegistryService.getSchemaIdForTopicAndVersion("test-topic", 999))
        .thenReturn(Optional.empty());

    ResponseEntity<Schema> response =
        controller.getSchemaByVersion("test-topic-value", "999", false);
    assertEquals(response.getStatusCode(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testErrorHandling_InvalidVersionFormat() {
    ResponseEntity<Schema> response =
        controller.getSchemaByVersion("test-topic-value", "invalid", false);
    assertEquals(response.getStatusCode(), HttpStatus.BAD_REQUEST);
  }

  // ============================================================================
  // SECTION 5: NEW FUNCTIONALITY TESTS
  // ============================================================================

  @Test
  public void testEventSchemaData_GetSchemaRegistryId() {
    // Test the new getSchemaRegistryId method
    EventSchemaData eventSchemaData = new EventSchemaData();

    // Test MCL schema registry IDs
    Integer mclV1Id = eventSchemaData.getSchemaRegistryId("MetadataChangeLog", 1);
    Integer mclV2Id = eventSchemaData.getSchemaRegistryId("MetadataChangeLog", 2);
    Integer mclV3Id = eventSchemaData.getSchemaRegistryId("MetadataChangeLog", 3);
    Integer mclV4Id = eventSchemaData.getSchemaRegistryId("MetadataChangeLog", 4);
    Integer mclV5Id = eventSchemaData.getSchemaRegistryId("MetadataChangeLog", 5);
    Integer mclV6Id = eventSchemaData.getSchemaRegistryId("MetadataChangeLog", 6);

    assertNotNull(mclV1Id, "MCL version 1 should have schema registry ID");
    assertNotNull(mclV2Id, "MCL version 2 should have schema registry ID");
    assertNotNull(mclV3Id, "MCL version 3 should have schema registry ID");
    assertNotNull(mclV4Id, "MCL version 4 should have schema registry ID");
    assertNotNull(mclV5Id, "MCL version 5 should have schema registry ID");
    assertNotNull(mclV6Id, "MCL version 6 should have schema registry ID");

    // Verify correct ordinal-based IDs
    assertEquals(mclV1Id, MCL_V1_SCHEMA_ID, "MCL version 1 should have schema ID 2");
    assertEquals(mclV2Id, MCL_TIMESERIES_V1_SCHEMA_ID, "MCL version 2 should have schema ID 3");
    assertEquals(mclV3Id, MCL_V1_FIX_SCHEMA_ID, "MCL version 3 should have schema ID 11");
    assertEquals(
        mclV4Id, MCL_TIMESERIES_V1_FIX_SCHEMA_ID, "MCL version 4 should have schema ID 12");
    assertEquals(mclV5Id, MCL_NEW_SCHEMA_ID, "MCL version 5 should have schema ID 18");
    assertEquals(mclV6Id, MCL_TIMESERIES_NEW_SCHEMA_ID, "MCL version 6 should have schema ID 19");

    // Test non-existent schema
    Integer nonExistentId = eventSchemaData.getSchemaRegistryId("NonExistentSchema", 1);
    assertNull(nonExistentId, "Non-existent schema should return null");

    // Test non-existent version
    Integer nonExistentVersionId = eventSchemaData.getSchemaRegistryId("MetadataChangeLog", 999);
    assertNull(nonExistentVersionId, "Non-existent version should return null");
  }

  @Test
  public void testSchemaRegistryService_GetSchemaIdForTopicAndVersion() {
    // Create real instances
    EventSchemaData eventSchemaData = new EventSchemaData();
    TopicConventionImpl topicConvention = new TopicConventionImpl();
    SchemaRegistryServiceImpl realService =
        new SchemaRegistryServiceImpl(topicConvention, eventSchemaData);

    // Test MCL topic with different versions
    String mclTopic = "MetadataChangeLog_Versioned_v1";

    // Test each version
    Optional<Integer> v1Id = realService.getSchemaIdForTopicAndVersion(mclTopic, 1);
    Optional<Integer> v2Id = realService.getSchemaIdForTopicAndVersion(mclTopic, 2);
    Optional<Integer> v3Id = realService.getSchemaIdForTopicAndVersion(mclTopic, 3);
    Optional<Integer> v4Id = realService.getSchemaIdForTopicAndVersion(mclTopic, 4);
    Optional<Integer> v5Id = realService.getSchemaIdForTopicAndVersion(mclTopic, 5);
    Optional<Integer> v6Id = realService.getSchemaIdForTopicAndVersion(mclTopic, 6);

    assertTrue(v1Id.isPresent(), "Version 1 should have schema ID");
    assertTrue(v2Id.isPresent(), "Version 2 should have schema ID");
    assertTrue(v3Id.isPresent(), "Version 3 should have schema ID");
    assertTrue(v4Id.isPresent(), "Version 4 should have schema ID");
    assertTrue(v5Id.isPresent(), "Version 5 should have schema ID");
    assertTrue(v6Id.isPresent(), "Version 6 should have schema ID");

    // Verify correct IDs
    assertEquals(v1Id.get(), MCL_V1_SCHEMA_ID, "Version 1 should have schema ID 2");
    assertEquals(v2Id.get(), MCL_TIMESERIES_V1_SCHEMA_ID, "Version 2 should have schema ID 3");
    assertEquals(v3Id.get(), MCL_V1_FIX_SCHEMA_ID, "Version 3 should have schema ID 11");
    assertEquals(v4Id.get(), MCL_TIMESERIES_V1_FIX_SCHEMA_ID, "Version 4 should have schema ID 12");
    assertEquals(v5Id.get(), MCL_NEW_SCHEMA_ID, "Version 5 should have schema ID 18");
    assertEquals(v6Id.get(), MCL_TIMESERIES_NEW_SCHEMA_ID, "Version 6 should have schema ID 19");

    // Test non-existent topic
    Optional<Integer> nonExistentTopicId =
        realService.getSchemaIdForTopicAndVersion("NonExistentTopic", 1);
    assertFalse(nonExistentTopicId.isPresent(), "Non-existent topic should return empty");

    // Test non-existent version
    Optional<Integer> nonExistentVersionId =
        realService.getSchemaIdForTopicAndVersion(mclTopic, 999);
    assertFalse(nonExistentVersionId.isPresent(), "Non-existent version should return empty");
  }

  @Test
  public void testEventSchemaConstants_Validation() {
    // Test that the validation works by ensuring all ordinals are mapped
    // This test will fail if any SchemaIdOrdinal enum value is missing from SCHEMA_ID_TO_SCHEMA_MAP

    // The validation happens during static initialization, so if we get here, it passed
    assertNotNull(EventSchemaConstants.SCHEMA_ID_TO_SCHEMA_MAP, "Schema map should be initialized");
    assertTrue(
        EventSchemaConstants.SCHEMA_ID_TO_SCHEMA_MAP.size() > 0, "Schema map should not be empty");

    // Verify that all the new _FIX ordinals are present
    assertTrue(
        EventSchemaConstants.SCHEMA_ID_TO_SCHEMA_MAP.containsKey(
            SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL_V1_FIX),
        "METADATA_CHANGE_PROPOSAL_V1_FIX should be mapped");
    assertTrue(
        EventSchemaConstants.SCHEMA_ID_TO_SCHEMA_MAP.containsKey(
            SchemaIdOrdinal.FAILED_METADATA_CHANGE_PROPOSAL_V1_FIX),
        "FAILED_METADATA_CHANGE_PROPOSAL_V1_FIX should be mapped");
    assertTrue(
        EventSchemaConstants.SCHEMA_ID_TO_SCHEMA_MAP.containsKey(
            SchemaIdOrdinal.METADATA_CHANGE_LOG_V1_FIX),
        "METADATA_CHANGE_LOG_V1_FIX should be mapped");
    assertTrue(
        EventSchemaConstants.SCHEMA_ID_TO_SCHEMA_MAP.containsKey(
            SchemaIdOrdinal.METADATA_CHANGE_LOG_TIMESERIES_V1_FIX),
        "METADATA_CHANGE_LOG_TIMESERIES_V1_FIX should be mapped");
    assertTrue(
        EventSchemaConstants.SCHEMA_ID_TO_SCHEMA_MAP.containsKey(
            SchemaIdOrdinal.METADATA_CHANGE_EVENT_V1_FIX),
        "METADATA_CHANGE_EVENT_V1_FIX should be mapped");
    assertTrue(
        EventSchemaConstants.SCHEMA_ID_TO_SCHEMA_MAP.containsKey(
            SchemaIdOrdinal.FAILED_METADATA_CHANGE_EVENT_V1_FIX),
        "FAILED_METADATA_CHANGE_EVENT_V1_FIX should be mapped");
    assertTrue(
        EventSchemaConstants.SCHEMA_ID_TO_SCHEMA_MAP.containsKey(
            SchemaIdOrdinal.METADATA_AUDIT_EVENT_V1_FIX),
        "METADATA_AUDIT_EVENT_V1_FIX should be mapped");

    // Verify that the _FIX ordinals map to the same schemas as their V1 counterparts
    assertEquals(
        EventSchemaConstants.SCHEMA_ID_TO_SCHEMA_MAP.get(
            SchemaIdOrdinal.METADATA_CHANGE_LOG_V1_FIX),
        EventSchemaConstants.SCHEMA_ID_TO_SCHEMA_MAP.get(SchemaIdOrdinal.METADATA_CHANGE_LOG_V1),
        "_FIX ordinal should map to same schema as V1");
    assertEquals(
        EventSchemaConstants.SCHEMA_ID_TO_SCHEMA_MAP.get(
            SchemaIdOrdinal.METADATA_CHANGE_LOG_TIMESERIES_V1_FIX),
        EventSchemaConstants.SCHEMA_ID_TO_SCHEMA_MAP.get(
            SchemaIdOrdinal.METADATA_CHANGE_LOG_TIMESERIES_V1),
        "_FIX ordinal should map to same schema as V1");
  }

  @Test
  public void testGetSchemas_AllVersions() {
    // Create real instances
    EventSchemaData eventSchemaData = new EventSchemaData();
    TopicConventionImpl topicConvention = new TopicConventionImpl();
    SchemaRegistryServiceImpl realService =
        new SchemaRegistryServiceImpl(topicConvention, eventSchemaData);

    SchemaRegistryController realController =
        new SchemaRegistryController(new ObjectMapper(), mockRequest, realService);

    // Test getSchemas without latestOnly (should return all versions)
    ResponseEntity<List<Schema>> allVersionsResponse =
        realController.getSchemas(null, false, false, null, null);
    assertEquals(allVersionsResponse.getStatusCode(), HttpStatus.OK);
    assertNotNull(allVersionsResponse.getBody());

    List<Schema> allSchemas = allVersionsResponse.getBody();
    assertTrue(allSchemas.size() > 0, "Should have some schemas");

    // Find MCL schemas
    List<Schema> mclSchemas =
        allSchemas.stream()
            .filter(schema -> MCL_VERSIONED_SUBJECT.equals(schema.getSubject()))
            .collect(java.util.stream.Collectors.toList());

    assertEquals(mclSchemas.size(), 6, "MCL should have 6 versions");

    // Verify each version has correct schema ID
    for (Schema schema : mclSchemas) {
      int version = schema.getVersion();
      int schemaId = schema.getId();

      switch (version) {
        case 1:
          assertEquals(schemaId, MCL_V1_SCHEMA_ID, "Version 1 should have schema ID 2");
          break;
        case 2:
          assertEquals(schemaId, MCL_TIMESERIES_V1_SCHEMA_ID, "Version 2 should have schema ID 3");
          break;
        case 3:
          assertEquals(schemaId, MCL_V1_FIX_SCHEMA_ID, "Version 3 should have schema ID 11");
          break;
        case 4:
          assertEquals(
              schemaId, MCL_TIMESERIES_V1_FIX_SCHEMA_ID, "Version 4 should have schema ID 12");
          break;
        case 5:
          assertEquals(schemaId, MCL_NEW_SCHEMA_ID, "Version 5 should have schema ID 18");
          break;
        case 6:
          assertEquals(
              schemaId, MCL_TIMESERIES_NEW_SCHEMA_ID, "Version 6 should have schema ID 19");
          break;
        default:
          fail("Unexpected version: " + version);
      }

      // Verify schema content exists
      assertNotNull(schema.getSchema(), "Schema content should not be null");
      assertTrue(schema.getSchema().contains("record"), "Schema should contain 'record'");
    }
  }

  @Test
  public void testGetSchemas_LatestOnly() {
    // Create real instances
    EventSchemaData eventSchemaData = new EventSchemaData();
    TopicConventionImpl topicConvention = new TopicConventionImpl();
    SchemaRegistryServiceImpl realService =
        new SchemaRegistryServiceImpl(topicConvention, eventSchemaData);

    SchemaRegistryController realController =
        new SchemaRegistryController(new ObjectMapper(), mockRequest, realService);

    // Test getSchemas with latestOnly=true (should return only latest versions)
    ResponseEntity<List<Schema>> latestOnlyResponse =
        realController.getSchemas(null, false, true, null, null);
    assertEquals(latestOnlyResponse.getStatusCode(), HttpStatus.OK);
    assertNotNull(latestOnlyResponse.getBody());

    List<Schema> latestSchemas = latestOnlyResponse.getBody();
    assertTrue(latestSchemas.size() > 0, "Should have some latest schemas");

    // Find MCL schema
    Optional<Schema> mclSchema =
        latestSchemas.stream()
            .filter(schema -> MCL_VERSIONED_SUBJECT.equals(schema.getSubject()))
            .findFirst();

    assertTrue(mclSchema.isPresent(), "MCL should have a latest schema");

    Schema mcl = mclSchema.get();
    assertEquals(mcl.getVersion(), 6, "Latest MCL version should be 6");
    assertEquals(mcl.getId(), MCL_TIMESERIES_NEW_SCHEMA_ID, "Latest MCL should have schema ID 19");
    assertNotNull(mcl.getSchema(), "Latest MCL schema content should not be null");
  }

  @Test
  public void testSchemaConfigLoader_OrdinalIdChanges() {
    // Test that the SchemaConfigLoader correctly loads ordinal_id instead of schema_id
    ObjectMapper objectMapper = new ObjectMapper();
    SchemaConfigLoader loader = new SchemaConfigLoader(objectMapper);
    SchemaConfigLoader.SchemaConfig config = loader.getConfig();

    assertNotNull(config, "Config should be loaded");
    assertNotNull(config.getSchemas(), "Schemas should be loaded");

    // Test MCL schema
    SchemaConfigLoader.SchemaDefinition mclDef = config.getSchemas().get("MetadataChangeLog");
    assertNotNull(mclDef, "MetadataChangeLog should be defined");

    List<SchemaConfigLoader.VersionDefinition> versions = mclDef.getVersions();
    assertEquals(versions.size(), 6, "MetadataChangeLog should have 6 versions");

    // Verify ordinal_id values
    assertEquals(
        versions.get(0).getOrdinalId(),
        "METADATA_CHANGE_LOG_V1",
        "Version 1 should have correct ordinal_id");
    assertEquals(
        versions.get(1).getOrdinalId(),
        "METADATA_CHANGE_LOG_TIMESERIES_V1",
        "Version 2 should have correct ordinal_id");
    assertEquals(
        versions.get(2).getOrdinalId(),
        "METADATA_CHANGE_LOG_V1_FIX",
        "Version 3 should have correct ordinal_id");
    assertEquals(
        versions.get(3).getOrdinalId(),
        "METADATA_CHANGE_LOG_TIMESERIES_V1_FIX",
        "Version 4 should have correct ordinal_id");
    assertEquals(
        versions.get(4).getOrdinalId(),
        "METADATA_CHANGE_LOG",
        "Version 5 should have correct ordinal_id");
    assertEquals(
        versions.get(5).getOrdinalId(),
        "METADATA_CHANGE_LOG_TIMESERIES",
        "Version 6 should have correct ordinal_id");
  }

  // ============================================================================
  // SECTION 6: COMPREHENSIVE VALIDATION TEST
  // ============================================================================

  @Test
  public void testComprehensiveSchemaRegistryFunctionality() {
    System.out.println("=== Comprehensive Schema Registry Test ===");

    // Create real instances
    EventSchemaData eventSchemaData = new EventSchemaData();
    TopicConventionImpl topicConvention = new TopicConventionImpl();
    SchemaRegistryServiceImpl realService =
        new SchemaRegistryServiceImpl(topicConvention, eventSchemaData);

    SchemaRegistryController realController =
        new SchemaRegistryController(new ObjectMapper(), mockRequest, realService);

    // 1. Test basic functionality
    testRealImplementation_BasicFunctionality();
    System.out.println("✅ Basic functionality works");

    // 2. Test subject listing
    testRealImplementation_ListAllSubjects();
    System.out.println("✅ Subject listing works");

    // 3. Test MCL schema validation
    testMCLSchema_AllVersionsExist();
    testMCLSchema_CorrectSchemaIds();
    testMCLSchema_LatestVersion();
    System.out.println("✅ MCL schema validation completed");

    // 4. Test new functionality
    testEventSchemaData_GetSchemaRegistryId();
    testSchemaRegistryService_GetSchemaIdForTopicAndVersion();
    testEventSchemaConstants_Validation();
    testSchemaConfigLoader_OrdinalIdChanges();
    System.out.println("✅ New functionality tests completed");

    // 5. Test getSchemas endpoint
    testGetSchemas_AllVersions();
    testGetSchemas_LatestOnly();
    System.out.println("✅ getSchemas endpoint tests completed");

    // 6. Test error handling
    testErrorHandling_NonExistentSubject();
    System.out.println("✅ Error handling works");

    System.out.println("=== All tests completed successfully! ===");
    System.out.println(
        "The schema registry is working correctly with the actual data configuration.");
  }
}
