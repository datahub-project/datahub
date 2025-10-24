package io.datahubproject.openapi.v3.controller;

import static com.linkedin.metadata.Constants.LOGICAL_PARENT_ASPECT_NAME;
import static com.linkedin.metadata.utils.GenericRecordUtils.JSON;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertEquals;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Edge;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.logical.LogicalParent;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.UpdateAspectResult;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.StringType;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.openapi.config.GlobalControllerExceptionHandler;
import io.datahubproject.openapi.config.SpringWebConfig;
import io.datahubproject.openapi.config.TracingInterceptor;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import org.apache.directory.scim.core.schema.SchemaRegistry;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SpringBootTest(classes = {SpringWebConfig.class})
@ComponentScan(basePackages = {"io.datahubproject.openapi.v3.controller.LogicalModelController"})
@Import({
  SpringWebConfig.class,
  TracingInterceptor.class,
  LogicalModelController.class,
  LogicalModelControllerTest.LogicalModelControllerTestConfig.class,
  GlobalControllerExceptionHandler.class, // ensure error responses
})
@AutoConfigureWebMvc
@AutoConfigureMockMvc
public class LogicalModelControllerTest extends AbstractTestNGSpringContextTests {
  @Autowired private LogicalModelController logicalModelController;
  @Autowired private MockMvc mockMvc;
  @Autowired private SearchService mockSearchService;
  @Autowired private EntityService<?> mockEntityService;
  @Autowired private TimeseriesAspectService mockTimeseriesAspectService;
  @Autowired private EntityRegistry entityRegistry;
  @Autowired private OperationContext opContext;
  @MockBean private ConfigurationProvider configurationProvider;

  @Captor private ArgumentCaptor<AspectsBatch> batchCaptor;

  @BeforeMethod
  public void setup() {
    org.mockito.MockitoAnnotations.openMocks(this);
  }

  @Test
  public void initTest() {
    assertNotNull(logicalModelController);
  }

  @TestConfiguration
  public static class LogicalModelControllerTestConfig {
    @MockBean public EntityServiceImpl entityService;
    @MockBean public SearchService searchService;
    @MockBean public TimeseriesAspectService timeseriesAspectService;
    @MockBean public SystemTelemetryContext systemTelemetryContext;

    @Bean
    public ObjectMapper objectMapper() {
      return new ObjectMapper();
    }

    @Bean(name = "systemOperationContext")
    public OperationContext systemOperationContext() {
      return TestOperationContexts.systemContextNoSearchAuthorization();
    }

    @Bean("entityRegistry")
    @Primary
    public EntityRegistry entityRegistry(
        @Qualifier("systemOperationContext") final OperationContext testOperationContext) {
      return testOperationContext.getEntityRegistry();
    }

    @Bean("graphService")
    @Primary
    public ElasticSearchGraphService graphService() {
      return mock(ElasticSearchGraphService.class);
    }

    @Bean
    public AuthorizerChain authorizerChain() {
      AuthorizerChain authorizerChain = mock(AuthorizerChain.class);

      Authentication authentication = mock(Authentication.class);
      when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "datahub"));
      when(authorizerChain.authorize(any()))
          .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
      AuthenticationContext.setAuthentication(authentication);

      return authorizerChain;
    }

    @Bean
    public TimeseriesAspectService timeseriesAspectService() {
      return timeseriesAspectService;
    }

    @MockBean public SchemaRegistry schemaRegistry;
  }

  @Test
  public void testSetLogicalParentsHappyPath() throws Exception {
    // Setup test data
    String childDatasetUrnStr = "urn:li:dataset:(urn:li:dataPlatform:bigquery,child_dataset,PROD)";
    String parentDatasetUrnStr =
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,parent_dataset,PROD)";
    Urn childDatasetUrn = UrnUtils.getUrn(childDatasetUrnStr);
    Urn parentDatasetUrn = UrnUtils.getUrn(parentDatasetUrnStr);

    // Create field path mapping
    Map<String, String> fieldPathMap =
        Map.of(
            "parent_field1", "child_field1",
            "parent_field2", "child_field2");
    String jsonBody = new ObjectMapper().writeValueAsString(fieldPathMap);

    // Create schema metadata with matching fields
    SchemaMetadata parentSchema = createSchemaMetadata("parent_field1", "parent_field2");
    SchemaMetadata childSchema = createSchemaMetadata("child_field1", "child_field2");

    // Mock entity service responses
    when(mockEntityService.getLatestAspect(any(), eq(parentDatasetUrn), eq("schemaMetadata")))
        .thenReturn(parentSchema);
    when(mockEntityService.getLatestAspect(any(), eq(childDatasetUrn), eq("schemaMetadata")))
        .thenReturn(childSchema);

    // Mock successful ingest with logicalParent aspects
    Urn childField1Urn = SchemaFieldUtils.generateSchemaFieldUrn(childDatasetUrn, "child_field1");
    Urn childField2Urn = SchemaFieldUtils.generateSchemaFieldUrn(childDatasetUrn, "child_field2");
    List<IngestResult> mockResults =
        List.of(
            createMockIngestResultWithLogicalParent(childDatasetUrn, parentDatasetUrn),
            createMockIngestResultWithLogicalParent(
                childField1Urn,
                SchemaFieldUtils.generateSchemaFieldUrn(parentDatasetUrn, "parent_field1")),
            createMockIngestResultWithLogicalParent(
                childField2Urn,
                SchemaFieldUtils.generateSchemaFieldUrn(parentDatasetUrn, "parent_field2")));
    when(mockEntityService.ingestProposal(any(), any(), eq(false))).thenReturn(mockResults);

    // Execute the request and verify response structure
    mockMvc
        .perform(
            MockMvcRequestBuilders.post(
                    "/openapi/v3/logical/"
                        + childDatasetUrnStr
                        + "/relationship/physicalInstanceOf/"
                        + parentDatasetUrnStr)
                .contentType(MediaType.APPLICATION_JSON)
                .content(jsonBody))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$").isArray())
        .andExpect(MockMvcResultMatchers.jsonPath("$.length()").value(3))
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].urn").value(childDatasetUrn.toString()))
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].logicalParent").exists())
        .andExpect(MockMvcResultMatchers.jsonPath("$[1].urn").value(childField1Urn.toString()))
        .andExpect(MockMvcResultMatchers.jsonPath("$[1].logicalParent").exists())
        .andExpect(MockMvcResultMatchers.jsonPath("$[2].urn").value(childField2Urn.toString()))
        .andExpect(MockMvcResultMatchers.jsonPath("$[2].logicalParent").exists());

    // Verify that ingestProposal was called
    verify(mockEntityService, times(1)).ingestProposal(any(), batchCaptor.capture(), eq(false));

    // Verify the batch contains the expected proposals (1 dataset + 2 schema fields)
    AspectsBatch capturedBatch = batchCaptor.getValue();
    assertEquals(3, capturedBatch.getMCPItems().size());
    MetadataChangeProposal mcp0 = capturedBatch.getMCPItems().get(0).getMetadataChangeProposal();
    LogicalParent aspect0 =
        GenericRecordUtils.deserializeAspect(
            mcp0.getAspect().getValue(), JSON, LogicalParent.class);
    assertEquals(childDatasetUrn, mcp0.getEntityUrn());
    assertEquals(LOGICAL_PARENT_ASPECT_NAME, mcp0.getAspectName());
    assertEquals(parentDatasetUrn, aspect0.getParent().getDestinationUrn());
    assertEquals(
        opContext.getActorContext().getActorUrn().toString(),
        aspect0.getParent().getLastModified().getActor().toString());

    MetadataChangeProposal mcp1 = capturedBatch.getMCPItems().get(1).getMetadataChangeProposal();
    LogicalParent aspect1 =
        GenericRecordUtils.deserializeAspect(
            mcp1.getAspect().getValue(), JSON, LogicalParent.class);
    assertEquals(childField1Urn, mcp1.getEntityUrn());
    assertEquals(LOGICAL_PARENT_ASPECT_NAME, mcp1.getAspectName());
    assertEquals(
        SchemaFieldUtils.generateSchemaFieldUrn(parentDatasetUrn, "parent_field1"),
        aspect1.getParent().getDestinationUrn());
    MetadataChangeProposal mcp2 = capturedBatch.getMCPItems().get(2).getMetadataChangeProposal();
    LogicalParent aspect2 =
        GenericRecordUtils.deserializeAspect(
            mcp2.getAspect().getValue(), JSON, LogicalParent.class);
    assertEquals(childField2Urn, mcp2.getEntityUrn());
    assertEquals(LOGICAL_PARENT_ASPECT_NAME, mcp2.getAspectName());
    assertEquals(
        SchemaFieldUtils.generateSchemaFieldUrn(parentDatasetUrn, "parent_field2"),
        aspect2.getParent().getDestinationUrn());
  }

  @Test
  public void testSetLogicalParentsWithoutSchemaMetadata() throws Exception {
    // Setup test data
    String childDatasetUrnStr = "urn:li:dataset:(urn:li:dataPlatform:bigquery,child_dataset,PROD)";
    String parentDatasetUrnStr =
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,parent_dataset,PROD)";
    Urn childDatasetUrn = UrnUtils.getUrn(childDatasetUrnStr);
    Urn parentDatasetUrn = UrnUtils.getUrn(parentDatasetUrnStr);

    // Create field path mapping
    Map<String, String> fieldPathMap = Map.of("parent_field1", "child_field1");
    String jsonBody = new ObjectMapper().writeValueAsString(fieldPathMap);

    // Mock entity service to return null for schema metadata (no schema validation)
    when(mockEntityService.getLatestAspect(any(), eq(parentDatasetUrn), eq("schemaMetadata")))
        .thenReturn(null);
    when(mockEntityService.getLatestAspect(any(), eq(childDatasetUrn), eq("schemaMetadata")))
        .thenReturn(null);

    // Execute the request - should succeed without schema validation
    mockMvc
        .perform(
            MockMvcRequestBuilders.post(
                    "/openapi/v3/logical/"
                        + childDatasetUrnStr
                        + "/relationship/physicalInstanceOf/"
                        + parentDatasetUrnStr)
                .contentType(MediaType.APPLICATION_JSON)
                .content(jsonBody))
        .andExpect(status().isOk());

    // Verify that ingestProposal was called
    verify(mockEntityService, times(1)).ingestProposal(any(), batchCaptor.capture(), eq(false));

    // Verify the batch contains the expected proposals (1 dataset + 1 schema field)
    AspectsBatch capturedBatch = batchCaptor.getValue();
    assertEquals(2, capturedBatch.getMCPItems().size());
  }

  @Test
  public void testSetLogicalParentsInvalidParentFieldPath() throws Exception {
    // Setup test data
    String childDatasetUrnStr = "urn:li:dataset:(urn:li:dataPlatform:bigquery,child_dataset,PROD)";
    String parentDatasetUrnStr =
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,parent_dataset,PROD)";
    Urn childDatasetUrn = UrnUtils.getUrn(childDatasetUrnStr);
    Urn parentDatasetUrn = UrnUtils.getUrn(parentDatasetUrnStr);

    // Create field path mapping with invalid parent field
    Map<String, String> fieldPathMap = Map.of("invalid_parent_field", "child_field1");
    String jsonBody = new ObjectMapper().writeValueAsString(fieldPathMap);

    // Create schema metadata - parent doesn't have the field referenced in mapping
    SchemaMetadata parentSchema = createSchemaMetadata("parent_field1", "parent_field2");
    SchemaMetadata childSchema = createSchemaMetadata("child_field1", "child_field2");

    // Mock entity service responses
    when(mockEntityService.getLatestAspect(any(), eq(parentDatasetUrn), eq("schemaMetadata")))
        .thenReturn(parentSchema);
    when(mockEntityService.getLatestAspect(any(), eq(childDatasetUrn), eq("schemaMetadata")))
        .thenReturn(childSchema);

    // Execute the request - should fail with validation error
    mockMvc
        .perform(
            MockMvcRequestBuilders.post(
                    "/openapi/v3/logical/"
                        + childDatasetUrnStr
                        + "/relationship/physicalInstanceOf/"
                        + parentDatasetUrnStr)
                .contentType(MediaType.APPLICATION_JSON)
                .content(jsonBody))
        .andExpect(status().isBadRequest());

    // Verify that ingestProposal was never called due to validation failure
    verify(mockEntityService, times(0)).ingestProposal(any(), any(), eq(false));
  }

  @Test
  public void testSetLogicalParentsInvalidChildFieldPath() throws Exception {
    // Setup test data
    String childDatasetUrnStr = "urn:li:dataset:(urn:li:dataPlatform:bigquery,child_dataset,PROD)";
    String parentDatasetUrnStr =
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,parent_dataset,PROD)";
    Urn childDatasetUrn = UrnUtils.getUrn(childDatasetUrnStr);
    Urn parentDatasetUrn = UrnUtils.getUrn(parentDatasetUrnStr);

    // Create field path mapping with invalid child field
    Map<String, String> fieldPathMap = Map.of("parent_field1", "invalid_child_field");
    String jsonBody = new ObjectMapper().writeValueAsString(fieldPathMap);

    // Create schema metadata - child doesn't have the field referenced in mapping
    SchemaMetadata parentSchema = createSchemaMetadata("parent_field1", "parent_field2");
    SchemaMetadata childSchema = createSchemaMetadata("child_field1", "child_field2");

    // Mock entity service responses
    when(mockEntityService.getLatestAspect(any(), eq(parentDatasetUrn), eq("schemaMetadata")))
        .thenReturn(parentSchema);
    when(mockEntityService.getLatestAspect(any(), eq(childDatasetUrn), eq("schemaMetadata")))
        .thenReturn(childSchema);

    // Execute the request - should fail with validation error
    mockMvc
        .perform(
            MockMvcRequestBuilders.post(
                    "/openapi/v3/logical/"
                        + childDatasetUrnStr
                        + "/relationship/physicalInstanceOf/"
                        + parentDatasetUrnStr)
                .contentType(MediaType.APPLICATION_JSON)
                .content(jsonBody))
        .andExpect(status().isBadRequest());

    // Verify that ingestProposal was never called due to validation failure
    verify(mockEntityService, times(0)).ingestProposal(any(), any(), eq(false));
  }

  // Helper methods for creating test data
  private SchemaMetadata createSchemaMetadata(String... fieldPaths) {
    SchemaFieldArray fields = new SchemaFieldArray();
    for (String fieldPath : fieldPaths) {
      SchemaField field =
          new SchemaField()
              .setFieldPath(fieldPath)
              .setNativeDataType("STRING")
              .setType(
                  new SchemaFieldDataType()
                      .setType(SchemaFieldDataType.Type.create(new StringType())));
      fields.add(field);
    }
    return new SchemaMetadata()
        .setSchemaName("test_schema")
        .setPlatform(new DataPlatformUrn("bigquery"))
        .setVersion(1L)
        .setFields(fields);
  }

  private IngestResult createMockIngestResultWithLogicalParent(Urn childUrn, Urn parentUrn) {
    // Create a LogicalParent aspect with Edge
    Edge edge =
        new Edge()
            .setDestinationUrn(parentUrn)
            .setCreated(
                new AuditStamp()
                    .setTime(System.currentTimeMillis())
                    .setActor(UrnUtils.getUrn("urn:li:corpuser:test")))
            .setLastModified(
                new AuditStamp()
                    .setTime(System.currentTimeMillis())
                    .setActor(UrnUtils.getUrn("urn:li:corpuser:test")));

    LogicalParent logicalParent = new LogicalParent().setParent(edge);

    // Create a mock MCPItem for the request field
    MCPItem mockRequest = mock(MCPItem.class);
    when(mockRequest.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockRequest.getAspectName()).thenReturn("logicalParent");
    when(mockRequest.getRecordTemplate()).thenReturn(logicalParent);
    when(mockRequest.getSystemMetadata()).thenReturn(new SystemMetadata());
    when(mockRequest.getAuditStamp())
        .thenReturn(
            new AuditStamp()
                .setTime(System.currentTimeMillis())
                .setActor(UrnUtils.getUrn("urn:li:corpuser:test")));

    return IngestResult.builder()
        .urn(childUrn)
        .sqlCommitted(true)
        .request(mockRequest)
        .result(
            UpdateAspectResult.builder()
                .urn(childUrn)
                .auditStamp(
                    new AuditStamp()
                        .setTime(System.currentTimeMillis())
                        .setActor(UrnUtils.getUrn("urn:li:corpuser:test")))
                .newValue(logicalParent)
                .newSystemMetadata(new SystemMetadata())
                .build())
        .build();
  }

  private IngestResult createMockIngestResultWithNullLogicalParent(Urn childUrn) {
    // Create a LogicalParent aspect with null parent (representing removal)
    LogicalParent logicalParent = new LogicalParent();

    // Create a mock MCPItem for the request field
    MCPItem mockRequest = mock(MCPItem.class);
    when(mockRequest.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockRequest.getAspectName()).thenReturn("logicalParent");
    when(mockRequest.getRecordTemplate()).thenReturn(logicalParent);
    when(mockRequest.getSystemMetadata()).thenReturn(new SystemMetadata());
    when(mockRequest.getAuditStamp())
        .thenReturn(
            new AuditStamp()
                .setTime(System.currentTimeMillis())
                .setActor(UrnUtils.getUrn("urn:li:corpuser:test")));

    return IngestResult.builder()
        .urn(childUrn)
        .sqlCommitted(true)
        .request(mockRequest)
        .result(
            UpdateAspectResult.builder()
                .urn(childUrn)
                .auditStamp(
                    new AuditStamp()
                        .setTime(System.currentTimeMillis())
                        .setActor(UrnUtils.getUrn("urn:li:corpuser:test")))
                .newValue(logicalParent)
                .newSystemMetadata(new SystemMetadata())
                .build())
        .build();
  }

  @Test
  public void testRemoveLogicalParentsHappyPath() throws Exception {
    // Setup test data
    String childDatasetUrnStr = "urn:li:dataset:(urn:li:dataPlatform:bigquery,child_dataset,PROD)";
    Urn childDatasetUrn = UrnUtils.getUrn(childDatasetUrnStr);

    // Create schema metadata with fields
    SchemaMetadata childSchema = createSchemaMetadata("child_field1", "child_field2");

    // Mock entity service response
    when(mockEntityService.getLatestAspect(any(), eq(childDatasetUrn), eq("schemaMetadata")))
        .thenReturn(childSchema);

    // Mock successful ingest with null logicalParent aspects (removal)
    Urn childField1Urn = SchemaFieldUtils.generateSchemaFieldUrn(childDatasetUrn, "child_field1");
    Urn childField2Urn = SchemaFieldUtils.generateSchemaFieldUrn(childDatasetUrn, "child_field2");
    List<IngestResult> mockResults =
        List.of(
            createMockIngestResultWithNullLogicalParent(childDatasetUrn),
            createMockIngestResultWithNullLogicalParent(childField1Urn),
            createMockIngestResultWithNullLogicalParent(childField2Urn));
    when(mockEntityService.ingestProposal(any(), any(), eq(false))).thenReturn(mockResults);

    // Execute the request and verify response structure
    mockMvc
        .perform(
            MockMvcRequestBuilders.delete(
                    "/openapi/v3/logical/"
                        + childDatasetUrnStr
                        + "/relationship/physicalInstanceOf")
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$").isArray())
        .andExpect(MockMvcResultMatchers.jsonPath("$.length()").value(3))
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].urn").value(childDatasetUrn.toString()))
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].logicalParent").exists())
        .andExpect(MockMvcResultMatchers.jsonPath("$[1].urn").value(childField1Urn.toString()))
        .andExpect(MockMvcResultMatchers.jsonPath("$[1].logicalParent").exists())
        .andExpect(MockMvcResultMatchers.jsonPath("$[2].urn").value(childField2Urn.toString()))
        .andExpect(MockMvcResultMatchers.jsonPath("$[2].logicalParent").exists());

    // Verify that ingestProposal was called
    verify(mockEntityService, times(1)).ingestProposal(any(), batchCaptor.capture(), eq(false));

    // Verify the batch contains the expected proposals (1 dataset + 2 schema fields)
    AspectsBatch capturedBatch = batchCaptor.getValue();
    assertEquals(3, capturedBatch.getMCPItems().size());

    // Verify dataset proposal has null parent
    MetadataChangeProposal mcp0 = capturedBatch.getMCPItems().get(0).getMetadataChangeProposal();
    LogicalParent aspect0 =
        GenericRecordUtils.deserializeAspect(
            mcp0.getAspect().getValue(), JSON, LogicalParent.class);
    assertEquals(childDatasetUrn, mcp0.getEntityUrn());
    assertEquals(LOGICAL_PARENT_ASPECT_NAME, mcp0.getAspectName());
    assertEquals(null, aspect0.getParent());

    // Verify schema field proposals
    MetadataChangeProposal mcp1 = capturedBatch.getMCPItems().get(1).getMetadataChangeProposal();
    LogicalParent aspect1 =
        GenericRecordUtils.deserializeAspect(
            mcp1.getAspect().getValue(), JSON, LogicalParent.class);
    assertEquals(childField1Urn, mcp1.getEntityUrn());
    assertEquals(LOGICAL_PARENT_ASPECT_NAME, mcp1.getAspectName());
    assertEquals(null, aspect1.getParent());

    MetadataChangeProposal mcp2 = capturedBatch.getMCPItems().get(2).getMetadataChangeProposal();
    LogicalParent aspect2 =
        GenericRecordUtils.deserializeAspect(
            mcp2.getAspect().getValue(), JSON, LogicalParent.class);
    assertEquals(childField2Urn, mcp2.getEntityUrn());
    assertEquals(LOGICAL_PARENT_ASPECT_NAME, mcp2.getAspectName());
    assertEquals(null, aspect2.getParent());
  }

  @Test
  public void testRemoveLogicalParentsWithoutSchemaMetadata() throws Exception {
    // Setup test data
    String childDatasetUrnStr = "urn:li:dataset:(urn:li:dataPlatform:bigquery,child_dataset,PROD)";
    Urn childDatasetUrn = UrnUtils.getUrn(childDatasetUrnStr);

    // Mock entity service to return null for schema metadata
    when(mockEntityService.getLatestAspect(any(), eq(childDatasetUrn), eq("schemaMetadata")))
        .thenReturn(null);

    // Mock successful ingest with only dataset removal (no schema fields)
    List<IngestResult> mockResults =
        List.of(createMockIngestResultWithNullLogicalParent(childDatasetUrn));
    when(mockEntityService.ingestProposal(any(), any(), eq(false))).thenReturn(mockResults);

    // Execute the request - should succeed with only dataset removal
    mockMvc
        .perform(
            MockMvcRequestBuilders.delete(
                    "/openapi/v3/logical/"
                        + childDatasetUrnStr
                        + "/relationship/physicalInstanceOf")
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$").isArray())
        .andExpect(MockMvcResultMatchers.jsonPath("$.length()").value(1))
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].urn").value(childDatasetUrn.toString()));

    // Verify that ingestProposal was called
    verify(mockEntityService, times(1)).ingestProposal(any(), batchCaptor.capture(), eq(false));

    // Verify the batch contains only the dataset proposal (no schema fields)
    AspectsBatch capturedBatch = batchCaptor.getValue();
    assertEquals(1, capturedBatch.getMCPItems().size());

    MetadataChangeProposal mcp0 = capturedBatch.getMCPItems().get(0).getMetadataChangeProposal();
    LogicalParent aspect0 =
        GenericRecordUtils.deserializeAspect(
            mcp0.getAspect().getValue(), JSON, LogicalParent.class);
    assertEquals(childDatasetUrn, mcp0.getEntityUrn());
    assertEquals(LOGICAL_PARENT_ASPECT_NAME, mcp0.getAspectName());
    assertEquals(null, aspect0.getParent());
  }

  @Test
  public void testRemoveLogicalParentsWithMultipleFields() throws Exception {
    // Setup test data with more fields to ensure all are processed
    String childDatasetUrnStr = "urn:li:dataset:(urn:li:dataPlatform:bigquery,child_dataset,PROD)";
    Urn childDatasetUrn = UrnUtils.getUrn(childDatasetUrnStr);

    // Create schema metadata with multiple fields
    SchemaMetadata childSchema =
        createSchemaMetadata("field1", "field2", "field3", "field4", "field5");

    // Mock entity service response
    when(mockEntityService.getLatestAspect(any(), eq(childDatasetUrn), eq("schemaMetadata")))
        .thenReturn(childSchema);

    // Mock successful ingest
    List<IngestResult> mockResults = new java.util.ArrayList<>();
    mockResults.add(createMockIngestResultWithNullLogicalParent(childDatasetUrn));
    for (int i = 1; i <= 5; i++) {
      Urn fieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(childDatasetUrn, "field" + i);
      mockResults.add(createMockIngestResultWithNullLogicalParent(fieldUrn));
    }
    when(mockEntityService.ingestProposal(any(), any(), eq(false))).thenReturn(mockResults);

    // Execute the request
    mockMvc
        .perform(
            MockMvcRequestBuilders.delete(
                    "/openapi/v3/logical/"
                        + childDatasetUrnStr
                        + "/relationship/physicalInstanceOf")
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$").isArray())
        .andExpect(MockMvcResultMatchers.jsonPath("$.length()").value(6));

    // Verify that ingestProposal was called
    verify(mockEntityService, times(1)).ingestProposal(any(), batchCaptor.capture(), eq(false));

    // Verify the batch contains all proposals (1 dataset + 5 schema fields)
    AspectsBatch capturedBatch = batchCaptor.getValue();
    assertEquals(6, capturedBatch.getMCPItems().size());
  }
}
