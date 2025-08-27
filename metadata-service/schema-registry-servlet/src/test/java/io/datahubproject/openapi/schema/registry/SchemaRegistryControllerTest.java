package io.datahubproject.openapi.schema.registry;

import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.registry.SchemaRegistryService;
import com.linkedin.mxe.TopicConvention;
import io.datahubproject.schema_registry.openapi.generated.Config;
import io.datahubproject.schema_registry.openapi.generated.RegisterSchemaRequest;
import io.datahubproject.schema_registry.openapi.generated.RegisterSchemaResponse;
import io.datahubproject.schema_registry.openapi.generated.Schema;
import io.datahubproject.schema_registry.openapi.generated.SchemaString;
import io.datahubproject.schema_registry.openapi.generated.SubjectVersion;
import java.util.List;
import java.util.Optional;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SchemaRegistryControllerTest {

  @Mock private SchemaRegistryService mockSchemaRegistryService;

  @Mock private TopicConvention mockTopicConvention;

  private SchemaRegistryController schemaRegistryController;
  private MockHttpServletRequest mockRequest;

  private static final String MCP_TOPIC = "MetadataChangeProposal";
  private static final String MCP_SUBJECT = "MetadataChangeProposal-value";
  private static final String FMCP_TOPIC = "FailedMetadataChangeProposal";
  private static final String FMCP_SUBJECT = "FailedMetadataChangeProposal-value";

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    mockRequest = new MockHttpServletRequest();
    schemaRegistryController =
        new SchemaRegistryController(new ObjectMapper(), mockRequest, mockSchemaRegistryService);
  }

  @Test
  public void testGetSchemaByVersion_Latest() {
    // Mock the service to return version 2 as latest
    when(mockSchemaRegistryService.getLatestSchemaVersionForTopic(MCP_TOPIC))
        .thenReturn(Optional.of(2));
    when(mockSchemaRegistryService.getSchemaForTopicAndVersion(MCP_TOPIC, 2))
        .thenReturn(Optional.of(createMockAvroSchema()));
    when(mockSchemaRegistryService.getSchemaIdForTopic(MCP_TOPIC)).thenReturn(Optional.of(1));

    ResponseEntity<Schema> response =
        schemaRegistryController.getSchemaByVersion(MCP_SUBJECT, "latest", false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertEquals(response.getBody().getVersion(), 2);
    Assert.assertEquals(response.getBody().getSubject(), MCP_SUBJECT);
  }

  @Test
  public void testGetSchemaByVersion_SpecificVersion() {
    // Mock the service to return version 1
    when(mockSchemaRegistryService.getSchemaForTopicAndVersion(MCP_TOPIC, 1))
        .thenReturn(Optional.of(createMockAvroSchema()));
    when(mockSchemaRegistryService.getSchemaIdForTopic(MCP_TOPIC)).thenReturn(Optional.of(1));

    ResponseEntity<Schema> response =
        schemaRegistryController.getSchemaByVersion(MCP_SUBJECT, "1", false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertEquals(response.getBody().getVersion(), 1);
    Assert.assertEquals(response.getBody().getSubject(), MCP_SUBJECT);
  }

  @Test
  public void testGetSchemaByVersion_InvalidVersion() {
    ResponseEntity<Schema> response =
        schemaRegistryController.getSchemaByVersion(MCP_SUBJECT, "3", false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testGetSchemaByVersion_InvalidVersionFormat() {
    ResponseEntity<Schema> response =
        schemaRegistryController.getSchemaByVersion(MCP_SUBJECT, "invalid", false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.BAD_REQUEST);
  }

  @Test
  public void testGetSchemaByVersion_TopicNotFound() {
    when(mockSchemaRegistryService.getSchemaForTopicAndVersion(MCP_TOPIC, 1))
        .thenReturn(Optional.empty());

    ResponseEntity<Schema> response =
        schemaRegistryController.getSchemaByVersion(MCP_SUBJECT, "1", false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testListVersions() {
    List<Integer> supportedVersions = List.of(1, 2);
    when(mockSchemaRegistryService.getSupportedSchemaVersionsForTopic(MCP_TOPIC))
        .thenReturn(Optional.of(supportedVersions));

    ResponseEntity<List<Integer>> response =
        schemaRegistryController.listVersions(MCP_SUBJECT, false, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertEquals(response.getBody(), supportedVersions);
  }

  @Test
  public void testListVersions_TopicNotFound() {
    when(mockSchemaRegistryService.getSupportedSchemaVersionsForTopic(MCP_TOPIC))
        .thenReturn(Optional.empty());

    ResponseEntity<List<Integer>> response =
        schemaRegistryController.listVersions(MCP_SUBJECT, false, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testList_AllSubjects() {
    // Mock the service to return a list of topics
    List<String> mockTopics =
        List.of(
            "MetadataChangeProposal",
            "FailedMetadataChangeProposal",
            "MetadataChangeLog",
            "PlatformEvent");
    when(mockSchemaRegistryService.getAllTopics()).thenReturn(mockTopics);

    ResponseEntity<List<String>> response = schemaRegistryController.list(null, false, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());

    List<String> subjects = response.getBody();
    Assert.assertEquals(subjects.size(), 4);

    // Verify subjects have "-value" suffix
    Assert.assertTrue(subjects.contains("MetadataChangeProposal-value"));
    Assert.assertTrue(subjects.contains("FailedMetadataChangeProposal-value"));
    Assert.assertTrue(subjects.contains("MetadataChangeLog-value"));
    Assert.assertTrue(subjects.contains("PlatformEvent-value"));
  }

  @Test
  public void testList_WithSubjectPrefix() {
    // Mock the service to return a list of topics
    List<String> mockTopics =
        List.of(
            "MetadataChangeProposal",
            "FailedMetadataChangeProposal",
            "MetadataChangeLog",
            "PlatformEvent");
    when(mockSchemaRegistryService.getAllTopics()).thenReturn(mockTopics);

    ResponseEntity<List<String>> response = schemaRegistryController.list("Metadata", false, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());

    List<String> subjects = response.getBody();
    Assert.assertEquals(subjects.size(), 2);

    // Should only contain subjects starting with "Metadata"
    Assert.assertTrue(subjects.contains("MetadataChangeProposal-value"));
    Assert.assertTrue(subjects.contains("MetadataChangeLog-value"));
    Assert.assertFalse(subjects.contains("FailedMetadataChangeProposal-value"));
    Assert.assertFalse(subjects.contains("PlatformEvent-value"));
  }

  @Test
  public void testList_WithExactSubjectPrefix() {
    // Mock the service to return a list of topics
    List<String> mockTopics =
        List.of(
            "MetadataChangeProposal",
            "FailedMetadataChangeProposal",
            "MetadataChangeLog",
            "PlatformEvent");
    when(mockSchemaRegistryService.getAllTopics()).thenReturn(mockTopics);

    ResponseEntity<List<String>> response =
        schemaRegistryController.list("MetadataChange", false, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());

    List<String> subjects = response.getBody();
    Assert.assertEquals(subjects.size(), 2);

    // Should only contain subjects starting with "MetadataChange"
    Assert.assertTrue(subjects.contains("MetadataChangeProposal-value"));
    Assert.assertTrue(subjects.contains("MetadataChangeLog-value"));
    Assert.assertFalse(subjects.contains("FailedMetadataChangeProposal-value"));
    Assert.assertFalse(subjects.contains("PlatformEvent-value"));
  }

  @Test
  public void testList_WithNoMatchingPrefix() {
    // Mock the service to return a list of topics
    List<String> mockTopics =
        List.of(
            "MetadataChangeProposal",
            "FailedMetadataChangeProposal",
            "MetadataChangeLog",
            "PlatformEvent");
    when(mockSchemaRegistryService.getAllTopics()).thenReturn(mockTopics);

    ResponseEntity<List<String>> response =
        schemaRegistryController.list("NonExistent", false, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());

    List<String> subjects = response.getBody();
    Assert.assertEquals(subjects.size(), 0);
  }

  @Test
  public void testList_DeletedTrue() {
    // Mock the service to return a list of topics
    List<String> mockTopics = List.of("MetadataChangeProposal", "FailedMetadataChangeProposal");
    when(mockSchemaRegistryService.getAllTopics()).thenReturn(mockTopics);

    ResponseEntity<List<String>> response = schemaRegistryController.list(null, true, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());

    List<String> subjects = response.getBody();
    Assert.assertEquals(subjects.size(), 0);
  }

  @Test
  public void testList_DeletedOnlyTrue() {
    // Mock the service to return a list of topics
    List<String> mockTopics = List.of("MetadataChangeProposal", "FailedMetadataChangeProposal");
    when(mockSchemaRegistryService.getAllTopics()).thenReturn(mockTopics);

    ResponseEntity<List<String>> response = schemaRegistryController.list(null, false, true);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());

    List<String> subjects = response.getBody();
    Assert.assertEquals(subjects.size(), 0);
  }

  @Test
  public void testList_DeletedTrueAndDeletedOnlyTrue() {
    // Mock the service to return a list of topics
    List<String> mockTopics = List.of("MetadataChangeProposal", "FailedMetadataChangeProposal");
    when(mockSchemaRegistryService.getAllTopics()).thenReturn(mockTopics);

    ResponseEntity<List<String>> response = schemaRegistryController.list(null, true, true);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());

    List<String> subjects = response.getBody();
    Assert.assertEquals(subjects.size(), 0);
  }

  @Test
  public void testList_EmptyTopicsList() {
    // Mock the service to return an empty list
    when(mockSchemaRegistryService.getAllTopics()).thenReturn(List.of());

    ResponseEntity<List<String>> response = schemaRegistryController.list(null, false, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());

    List<String> subjects = response.getBody();
    Assert.assertEquals(subjects.size(), 0);
  }

  @Test
  public void testList_WithSpecialCharactersInTopics() {
    // Mock the service to return topics with special characters
    List<String> mockTopics =
        List.of(
            "Metadata-Change.Proposal", "Failed_Metadata_Change_Proposal", "MetadataChangeLog123");
    when(mockSchemaRegistryService.getAllTopics()).thenReturn(mockTopics);

    ResponseEntity<List<String>> response = schemaRegistryController.list(null, false, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());

    List<String> subjects = response.getBody();
    Assert.assertEquals(subjects.size(), 3);

    // Verify subjects have "-value" suffix
    Assert.assertTrue(subjects.contains("Metadata-Change.Proposal-value"));
    Assert.assertTrue(subjects.contains("Failed_Metadata_Change_Proposal-value"));
    Assert.assertTrue(subjects.contains("MetadataChangeLog123-value"));
  }

  @Test
  public void testList_ServiceReturnsNull() {
    // Mock the service to return null (edge case)
    when(mockSchemaRegistryService.getAllTopics()).thenReturn(null);

    ResponseEntity<List<String>> response = schemaRegistryController.list(null, false, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());

    List<String> subjects = response.getBody();
    Assert.assertEquals(subjects.size(), 0);
  }

  @Test
  public void testGetSchemaByVersion_FailedMCPTopic() {
    // Test that Failed MCP topic also works with versioning
    when(mockSchemaRegistryService.getSchemaForTopicAndVersion(FMCP_TOPIC, 1))
        .thenReturn(Optional.of(createMockAvroSchema()));
    when(mockSchemaRegistryService.getSchemaIdForTopic(FMCP_TOPIC)).thenReturn(Optional.of(2));

    ResponseEntity<Schema> response =
        schemaRegistryController.getSchemaByVersion(FMCP_SUBJECT, "1", false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertEquals(response.getBody().getVersion(), 1);
    Assert.assertEquals(response.getBody().getSubject(), FMCP_SUBJECT);
  }

  @Test
  public void testGetSchemaByVersion_UnsupportedVersion() {
    // Test with a version that's not in the supported set
    ResponseEntity<Schema> response =
        schemaRegistryController.getSchemaByVersion(MCP_SUBJECT, "99", false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testGetSchemaByVersion_EmptyVersion() {
    ResponseEntity<Schema> response =
        schemaRegistryController.getSchemaByVersion(MCP_SUBJECT, "", false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.BAD_REQUEST);
  }

  @Test
  public void testGetSchemaByVersion_NullVersion() {
    ResponseEntity<Schema> response =
        schemaRegistryController.getSchemaByVersion(MCP_SUBJECT, null, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.BAD_REQUEST);
  }

  // New tests for missing methods

  @Test
  public void testGetSchema_Success() {
    // Test the critical getSchema method for Kafka consumer deserialization
    Integer schemaId = 1;
    String topicName = "MetadataChangeProposal";
    org.apache.avro.Schema mockAvroSchema = createMockAvroSchema();

    when(mockSchemaRegistryService.getTopicNameById(schemaId)).thenReturn(Optional.of(topicName));
    when(mockSchemaRegistryService.getSchemaForId(schemaId))
        .thenReturn(Optional.of(mockAvroSchema));
    when(mockSchemaRegistryService.getLatestSchemaVersionForTopic(topicName))
        .thenReturn(Optional.of(1));

    ResponseEntity<SchemaString> response =
        schemaRegistryController.getSchema(schemaId, null, null, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());
    Assert.assertEquals(response.getBody().getSchema(), mockAvroSchema.toString());
    Assert.assertEquals(response.getBody().getSchemaType(), "AVRO");
    Assert.assertNull(response.getBody().getMaxId());
  }

  @Test
  public void testGetSchema_WithFetchMaxId() {
    // Test getSchema with fetchMaxId=true
    Integer schemaId = 1;
    String topicName = "MetadataChangeProposal";
    org.apache.avro.Schema mockAvroSchema = createMockAvroSchema();

    when(mockSchemaRegistryService.getTopicNameById(schemaId)).thenReturn(Optional.of(topicName));
    when(mockSchemaRegistryService.getSchemaForId(schemaId))
        .thenReturn(Optional.of(mockAvroSchema));
    when(mockSchemaRegistryService.getLatestSchemaVersionForTopic(topicName))
        .thenReturn(Optional.of(1));

    ResponseEntity<SchemaString> response =
        schemaRegistryController.getSchema(schemaId, null, null, true);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());
    Assert.assertEquals(response.getBody().getMaxId(), schemaId);
  }

  @Test
  public void testGetSchema_SchemaIdNotFound() {
    // Test getSchema when schema ID doesn't exist
    Integer schemaId = 999;
    when(mockSchemaRegistryService.getTopicNameById(schemaId)).thenReturn(Optional.empty());

    ResponseEntity<SchemaString> response =
        schemaRegistryController.getSchema(schemaId, null, null, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testGetSchema_SchemaNotFound() {
    // Test getSchema when schema exists but schema content not found
    Integer schemaId = 1;
    String topicName = "MetadataChangeProposal";

    when(mockSchemaRegistryService.getTopicNameById(schemaId)).thenReturn(Optional.of(topicName));
    when(mockSchemaRegistryService.getSchemaForId(schemaId)).thenReturn(Optional.empty());

    ResponseEntity<SchemaString> response =
        schemaRegistryController.getSchema(schemaId, null, null, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testGetSchema_NoVersionFound() {
    // Test getSchema when no version found for topic
    Integer schemaId = 1;
    String topicName = "MetadataChangeProposal";
    org.apache.avro.Schema mockAvroSchema = createMockAvroSchema();

    when(mockSchemaRegistryService.getTopicNameById(schemaId)).thenReturn(Optional.of(topicName));
    when(mockSchemaRegistryService.getSchemaForId(schemaId))
        .thenReturn(Optional.of(mockAvroSchema));
    when(mockSchemaRegistryService.getLatestSchemaVersionForTopic(topicName))
        .thenReturn(Optional.empty());

    ResponseEntity<SchemaString> response =
        schemaRegistryController.getSchema(schemaId, null, null, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testGetSchemaOnly2_Success() {
    // Test getSchemaOnly2 method
    String subject = "MetadataChangeProposal-value";
    String version = "1";
    org.apache.avro.Schema mockAvroSchema = createMockAvroSchema();

    when(mockSchemaRegistryService.getSchemaBySubjectAndVersion(subject, 1))
        .thenReturn(Optional.of(mockAvroSchema));

    ResponseEntity<String> response =
        schemaRegistryController.getSchemaOnly2(subject, version, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertEquals(response.getBody(), mockAvroSchema.toString());
  }

  @Test
  public void testGetSchemaOnly2_FallbackToTopicLookup() {
    // Test getSchemaOnly2 fallback to topic-based lookup
    String subject = "MetadataChangeProposal-value";
    String version = "1";
    String topicName = "MetadataChangeProposal";
    org.apache.avro.Schema mockAvroSchema = createMockAvroSchema();

    when(mockSchemaRegistryService.getSchemaBySubjectAndVersion(subject, 1))
        .thenReturn(Optional.empty());
    when(mockSchemaRegistryService.getSchemaForTopicAndVersion(topicName, 1))
        .thenReturn(Optional.of(mockAvroSchema));

    ResponseEntity<String> response =
        schemaRegistryController.getSchemaOnly2(subject, version, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertEquals(response.getBody(), mockAvroSchema.toString());
  }

  @Test
  public void testGetSchemaOnly2_InvalidVersionFormat() {
    // Test getSchemaOnly2 with invalid version format
    String subject = "MetadataChangeProposal-value";
    String version = "invalid";

    ResponseEntity<String> response =
        schemaRegistryController.getSchemaOnly2(subject, version, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.BAD_REQUEST);
  }

  @Test
  public void testGetSchemaOnly2_SchemaNotFound() {
    // Test getSchemaOnly2 when schema not found
    String subject = "MetadataChangeProposal-value";
    String version = "1";
    String topicName = "MetadataChangeProposal";

    when(mockSchemaRegistryService.getSchemaBySubjectAndVersion(subject, 1))
        .thenReturn(Optional.empty());
    when(mockSchemaRegistryService.getSchemaForTopicAndVersion(topicName, 1))
        .thenReturn(Optional.empty());

    ResponseEntity<String> response =
        schemaRegistryController.getSchemaOnly2(subject, version, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testRegister_Success() {
    // Test register method
    String subject = "MetadataChangeProposal-value";
    String topicName = "MetadataChangeProposal";
    Integer schemaId = 1;
    RegisterSchemaRequest request = new RegisterSchemaRequest();

    when(mockSchemaRegistryService.getSchemaIdForTopic(topicName))
        .thenReturn(Optional.of(schemaId));

    ResponseEntity<RegisterSchemaResponse> response =
        schemaRegistryController.register(subject, request, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());
    Assert.assertEquals(response.getBody().getId(), schemaId);
  }

  @Test
  public void testRegister_TopicNotFound() {
    // Test register method when topic not found
    String subject = "MetadataChangeProposal-value";
    String topicName = "MetadataChangeProposal";
    RegisterSchemaRequest request = new RegisterSchemaRequest();

    when(mockSchemaRegistryService.getSchemaIdForTopic(topicName)).thenReturn(Optional.empty());

    ResponseEntity<RegisterSchemaResponse> response =
        schemaRegistryController.register(subject, request, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testRegister_MalformedTopicName() {
    // Test register method with malformed topic name
    String subject = "Invalid@Topic-value";
    RegisterSchemaRequest request = new RegisterSchemaRequest();

    ResponseEntity<RegisterSchemaResponse> response =
        schemaRegistryController.register(subject, request, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testGetSchemaOnly_Success() {
    // Test getSchemaOnly method
    Integer schemaId = 1;
    org.apache.avro.Schema mockAvroSchema = createMockAvroSchema();

    when(mockSchemaRegistryService.getSchemaForId(schemaId))
        .thenReturn(Optional.of(mockAvroSchema));

    ResponseEntity<String> response = schemaRegistryController.getSchemaOnly(schemaId, null, null);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertEquals(response.getBody(), mockAvroSchema.toString());
  }

  @Test
  public void testGetSchemaOnly_SchemaNotFound() {
    // Test getSchemaOnly method when schema not found
    Integer schemaId = 999;

    when(mockSchemaRegistryService.getSchemaForId(schemaId)).thenReturn(Optional.empty());

    ResponseEntity<String> response = schemaRegistryController.getSchemaOnly(schemaId, null, null);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testGetSchemaTypes() {
    // Test getSchemaTypes method
    ResponseEntity<List<String>> response = schemaRegistryController.getSchemaTypes();

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());
    Assert.assertEquals(response.getBody().size(), 1);
    Assert.assertTrue(response.getBody().contains("AVRO"));
  }

  @Test
  public void testGetSchemas_Success() {
    // Test getSchemas method
    List<String> mockTopics = List.of("MetadataChangeProposal", "MetadataChangeLog");
    org.apache.avro.Schema mockAvroSchema = createMockAvroSchema();

    when(mockSchemaRegistryService.getAllTopics()).thenReturn(mockTopics);
    when(mockSchemaRegistryService.getSchemaIdForTopic("MetadataChangeProposal"))
        .thenReturn(Optional.of(1));
    when(mockSchemaRegistryService.getSchemaForTopic("MetadataChangeProposal"))
        .thenReturn(Optional.of(mockAvroSchema));
    when(mockSchemaRegistryService.getSchemaIdForTopic("MetadataChangeLog"))
        .thenReturn(Optional.of(2));
    when(mockSchemaRegistryService.getSchemaForTopic("MetadataChangeLog"))
        .thenReturn(Optional.of(mockAvroSchema));

    ResponseEntity<List<Schema>> response =
        schemaRegistryController.getSchemas(null, false, false, null, null);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());
    Assert.assertEquals(response.getBody().size(), 2);
  }

  @Test
  public void testGetSchemas_WithDeleted() {
    // Test getSchemas method with deleted=true
    ResponseEntity<List<Schema>> response =
        schemaRegistryController.getSchemas(null, true, false, null, null);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());
    Assert.assertEquals(response.getBody().size(), 0);
  }

  @Test
  public void testGetSchemas_WithLatestOnly() {
    // Test getSchemas method with latestOnly=true
    List<String> mockTopics = List.of("MetadataChangeProposal");
    org.apache.avro.Schema mockAvroSchema = createMockAvroSchema();

    when(mockSchemaRegistryService.getAllTopics()).thenReturn(mockTopics);
    when(mockSchemaRegistryService.getSchemaIdForTopic("MetadataChangeProposal"))
        .thenReturn(Optional.of(1));
    when(mockSchemaRegistryService.getSchemaForTopic("MetadataChangeProposal"))
        .thenReturn(Optional.of(mockAvroSchema));
    when(mockSchemaRegistryService.getLatestSchemaVersionForTopic("MetadataChangeProposal"))
        .thenReturn(Optional.of(2));

    ResponseEntity<List<Schema>> response =
        schemaRegistryController.getSchemas(null, false, true, null, null);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());
    Assert.assertEquals(response.getBody().size(), 1);
    Assert.assertEquals(response.getBody().get(0).getVersion(), 2);
  }

  @Test
  public void testGetSchemas_WithOffsetAndLimit() {
    // Test getSchemas method with offset and limit
    List<String> mockTopics =
        List.of("MetadataChangeProposal", "MetadataChangeLog", "PlatformEvent");
    org.apache.avro.Schema mockAvroSchema = createMockAvroSchema();

    when(mockSchemaRegistryService.getAllTopics()).thenReturn(mockTopics);
    when(mockSchemaRegistryService.getSchemaIdForTopic("MetadataChangeProposal"))
        .thenReturn(Optional.of(1));
    when(mockSchemaRegistryService.getSchemaForTopic("MetadataChangeProposal"))
        .thenReturn(Optional.of(mockAvroSchema));
    when(mockSchemaRegistryService.getSchemaIdForTopic("MetadataChangeLog"))
        .thenReturn(Optional.of(2));
    when(mockSchemaRegistryService.getSchemaForTopic("MetadataChangeLog"))
        .thenReturn(Optional.of(mockAvroSchema));
    when(mockSchemaRegistryService.getSchemaIdForTopic("PlatformEvent")).thenReturn(Optional.of(3));
    when(mockSchemaRegistryService.getSchemaForTopic("PlatformEvent"))
        .thenReturn(Optional.of(mockAvroSchema));

    ResponseEntity<List<Schema>> response =
        schemaRegistryController.getSchemas(null, false, false, 1, 2);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());
    Assert.assertEquals(response.getBody().size(), 2);
  }

  @Test
  public void testGetSchemas_WithSubjectPrefix() {
    // Test getSchemas method with subject prefix
    List<String> mockTopics =
        List.of("MetadataChangeProposal", "MetadataChangeLog", "PlatformEvent");
    org.apache.avro.Schema mockAvroSchema = createMockAvroSchema();

    when(mockSchemaRegistryService.getAllTopics()).thenReturn(mockTopics);
    when(mockSchemaRegistryService.getSchemaIdForTopic("MetadataChangeProposal"))
        .thenReturn(Optional.of(1));
    when(mockSchemaRegistryService.getSchemaForTopic("MetadataChangeProposal"))
        .thenReturn(Optional.of(mockAvroSchema));
    when(mockSchemaRegistryService.getSchemaIdForTopic("MetadataChangeLog"))
        .thenReturn(Optional.of(2));
    when(mockSchemaRegistryService.getSchemaForTopic("MetadataChangeLog"))
        .thenReturn(Optional.of(mockAvroSchema));

    ResponseEntity<List<Schema>> response =
        schemaRegistryController.getSchemas("Metadata", false, false, null, null);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());
    Assert.assertEquals(response.getBody().size(), 2);
  }

  @Test
  public void testGetSubjects_BySchemaId() {
    // Test getSubjects method with schema ID
    Integer schemaId = 1;
    String topicName = "MetadataChangeProposal";

    when(mockSchemaRegistryService.getTopicNameById(schemaId)).thenReturn(Optional.of(topicName));

    ResponseEntity<List<String>> response =
        schemaRegistryController.getSubjects(schemaId, null, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());
    Assert.assertEquals(response.getBody().size(), 1);
    Assert.assertEquals(response.getBody().get(0), "MetadataChangeProposal-value");
  }

  @Test
  public void testGetSubjects_BySchemaIdNotFound() {
    // Test getSubjects method with non-existent schema ID
    Integer schemaId = 999;

    when(mockSchemaRegistryService.getTopicNameById(schemaId)).thenReturn(Optional.empty());

    ResponseEntity<List<String>> response =
        schemaRegistryController.getSubjects(schemaId, null, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testGetSubjects_BySubject() {
    // Test getSubjects method with subject name
    String subject = "MetadataChangeProposal-value";
    String topicName = "MetadataChangeProposal";
    Integer schemaId = 1;

    when(mockSchemaRegistryService.getSchemaIdForTopic(topicName))
        .thenReturn(Optional.of(schemaId));

    ResponseEntity<List<String>> response =
        schemaRegistryController.getSubjects(null, subject, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());
    Assert.assertEquals(response.getBody().size(), 1);
    Assert.assertEquals(response.getBody().get(0), subject);
  }

  @Test
  public void testGetSubjects_BySubjectNotFound() {
    // Test getSubjects method with non-existent subject
    String subject = "NonExistent-value";
    String topicName = "NonExistent";

    when(mockSchemaRegistryService.getSchemaIdForTopic(topicName)).thenReturn(Optional.empty());

    ResponseEntity<List<String>> response =
        schemaRegistryController.getSubjects(null, subject, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testGetSubjects_AllSubjects() {
    // Test getSubjects method without parameters
    List<String> mockTopics = List.of("MetadataChangeProposal", "MetadataChangeLog");

    when(mockSchemaRegistryService.getAllTopics()).thenReturn(mockTopics);

    ResponseEntity<List<String>> response = schemaRegistryController.getSubjects(null, null, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());
    Assert.assertEquals(response.getBody().size(), 2);
    Assert.assertTrue(response.getBody().contains("MetadataChangeProposal-value"));
    Assert.assertTrue(response.getBody().contains("MetadataChangeLog-value"));
  }

  @Test
  public void testGetSubjects_WithDeleted() {
    // Test getSubjects method with deleted=true
    ResponseEntity<List<String>> response = schemaRegistryController.getSubjects(null, null, true);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());
    Assert.assertEquals(response.getBody().size(), 0);
  }

  @Test
  public void testGetVersions_BySubject() {
    // Test getVersions method with subject
    String subject = "MetadataChangeProposal-value";
    String topicName = "MetadataChangeProposal";
    List<Integer> supportedVersions = List.of(1, 2);

    when(mockSchemaRegistryService.getSupportedSchemaVersionsForTopic(topicName))
        .thenReturn(Optional.of(supportedVersions));

    ResponseEntity<List<SubjectVersion>> response =
        schemaRegistryController.getVersions(null, subject, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());
    Assert.assertEquals(response.getBody().size(), 2);
    Assert.assertEquals(response.getBody().get(0).getSubject(), subject);
    Assert.assertEquals(response.getBody().get(0).getVersion(), 1);
    Assert.assertEquals(response.getBody().get(1).getSubject(), subject);
    Assert.assertEquals(response.getBody().get(1).getVersion(), 2);
  }

  @Test
  public void testGetVersions_BySubjectNotFound() {
    // Test getVersions method with non-existent subject
    String subject = "NonExistent-value";
    String topicName = "NonExistent";

    when(mockSchemaRegistryService.getSupportedSchemaVersionsForTopic(topicName))
        .thenReturn(Optional.empty());

    ResponseEntity<List<SubjectVersion>> response =
        schemaRegistryController.getVersions(null, subject, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testGetVersions_BySchemaId() {
    // Test getVersions method with schema ID
    Integer schemaId = 1;
    String topicName = "MetadataChangeProposal";
    List<Integer> supportedVersions = List.of(1, 2);

    when(mockSchemaRegistryService.getTopicNameById(schemaId)).thenReturn(Optional.of(topicName));
    when(mockSchemaRegistryService.getSupportedSchemaVersionsForTopic(topicName))
        .thenReturn(Optional.of(supportedVersions));

    ResponseEntity<List<SubjectVersion>> response =
        schemaRegistryController.getVersions(schemaId, null, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());
    Assert.assertEquals(response.getBody().size(), 2);
    Assert.assertEquals(response.getBody().get(0).getSubject(), "MetadataChangeProposal-value");
  }

  @Test
  public void testGetVersions_BySchemaIdNotFound() {
    // Test getVersions method with non-existent schema ID
    Integer schemaId = 999;

    when(mockSchemaRegistryService.getTopicNameById(schemaId)).thenReturn(Optional.empty());

    ResponseEntity<List<SubjectVersion>> response =
        schemaRegistryController.getVersions(schemaId, null, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testGetVersions_NoParameters() {
    // Test getVersions method without parameters
    ResponseEntity<List<SubjectVersion>> response =
        schemaRegistryController.getVersions(null, null, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());
    Assert.assertEquals(response.getBody().size(), 0);
  }

  @Test
  public void testGetVersions_WithDeleted() {
    // Test getVersions method with deleted=true
    ResponseEntity<List<SubjectVersion>> response =
        schemaRegistryController.getVersions(null, null, true);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());
    Assert.assertEquals(response.getBody().size(), 0);
  }

  @Test
  public void testGetReferencedBy() {
    // Test getReferencedBy method - should always return empty list
    ResponseEntity<List<Integer>> response =
        schemaRegistryController.getReferencedBy("test-subject", "1");

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());
    Assert.assertEquals(response.getBody().size(), 0);
  }

  @Test
  public void testGetSubjectLevelConfig_Success() {
    // Test getSubjectLevelConfig method
    String subject = "MetadataChangeProposal-value";
    String topicName = "MetadataChangeProposal";
    String compatibilityLevel = "BACKWARD";

    when(mockSchemaRegistryService.getSchemaCompatibility(topicName))
        .thenReturn(compatibilityLevel);

    ResponseEntity<Config> response =
        schemaRegistryController.getSubjectLevelConfig(subject, false);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());
    Assert.assertEquals(response.getBody().getCompatibilityLevel().toString(), compatibilityLevel);
  }

  @Test
  public void testGetSubjectLevelConfig_WithDefaultToGlobal() {
    // Test getSubjectLevelConfig method with defaultToGlobal=true
    String subject = "MetadataChangeProposal-value";
    String topicName = "MetadataChangeProposal";

    when(mockSchemaRegistryService.getSchemaCompatibility(topicName)).thenReturn(null);

    ResponseEntity<Config> response = schemaRegistryController.getSubjectLevelConfig(subject, true);

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());
    Assert.assertEquals(response.getBody().getCompatibilityLevel().toString(), "BACKWARD");
  }

  @Test
  public void testGetTopLevelConfig() {
    // Test getTopLevelConfig method
    ResponseEntity<Config> response = schemaRegistryController.getTopLevelConfig();

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
    Assert.assertNotNull(response.getBody());
    Assert.assertEquals(response.getBody().getCompatibilityLevel().toString(), "BACKWARD");
  }

  @Test
  public void testGet() {
    // Test get method (root endpoint)
    ResponseEntity<String> response = schemaRegistryController.get();

    Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
  }

  private org.apache.avro.Schema createMockAvroSchema() {
    // Create a simple mock Avro schema for testing
    return org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);
  }
}
