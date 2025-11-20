package com.linkedin.datahub.graphql.resolvers.mutate;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.generated.PatchEntityInput;
import com.linkedin.datahub.graphql.generated.PatchEntityResult;
import com.linkedin.datahub.graphql.generated.PatchOperationInput;
import com.linkedin.datahub.graphql.generated.PatchOperationType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PatchEntitiesResolverTest {

  private EntityService _entityService;
  private EntityClient _entityClient;
  private EntityRegistry _entityRegistry;
  private ObjectMapper _objectMapper;
  private PatchEntitiesResolver _resolver;
  private DataFetchingEnvironment _environment;
  private QueryContext _context;
  private Authentication _authentication;
  private OperationContext _operationContext;

  @BeforeMethod
  public void setup() {
    _entityService = mock(EntityService.class);
    _entityClient = mock(EntityClient.class);
    _entityRegistry = mock(EntityRegistry.class);
    _objectMapper = new ObjectMapper();
    _resolver = new PatchEntitiesResolver(_entityService, _entityClient, _entityRegistry);

    _environment = mock(DataFetchingEnvironment.class);
    _context = TestUtils.getMockAllowContext("urn:li:corpuser:test-user");
    _authentication = _context.getAuthentication();
    _operationContext = _context.getOperationContext();

    when(_environment.getContext()).thenReturn(_context);
  }

  @Test
  public void testPatchEntitiesSuccess() throws Exception {
    // Arrange - Test batch patch with multiple entities
    PatchEntityInput input1 =
        createGlossaryTermInput("urn:li:glossaryTerm:test-term-1", "Test Term 1");
    PatchEntityInput input2 =
        createGlossaryTermInput("urn:li:glossaryTerm:test-term-2", "Test Term 2");
    PatchEntityInput[] inputArray = {input1, input2};

    when(_environment.getArgument("input")).thenReturn(inputArray);

    // Mock entity registry
    setupEntityRegistryMocks();

    // Authorization is already set up by TestUtils.getMockAllowContext()

    // Mock successful batch ingestion
    when(_entityService.ingestProposal(any(), any(), eq(false)))
        .thenReturn(Arrays.asList()); // EntityUtils.ingestChangeProposals calls ingestProposal

    // Act
    CompletableFuture<List<PatchEntityResult>> future = _resolver.get(_environment);
    List<PatchEntityResult> results = future.get();

    // Assert
    assertNotNull(results);
    assertEquals(results.size(), 2);

    // Check first result
    PatchEntityResult result1 = results.get(0);
    assertNotNull(result1);
    assertEquals(result1.getUrn(), "urn:li:glossaryTerm:test-term-1");
    assertEquals(result1.getName(), "\"Test Term 1\"");
    assertTrue(result1.getSuccess());
    assertNull(result1.getError());

    // Check second result
    PatchEntityResult result2 = results.get(1);
    assertNotNull(result2);
    assertEquals(result2.getUrn(), "urn:li:glossaryTerm:test-term-2");
    assertEquals(result2.getName(), "\"Test Term 2\"");
    assertTrue(result2.getSuccess());
    assertNull(result2.getError());
  }

  @Test
  public void testPatchEntitiesWithCustomProperties() throws Exception {
    // Arrange - Test batch patch with custom properties (key-value pairs)
    PatchEntityInput input =
        createGlossaryTermWithCustomProperties("urn:li:glossaryTerm:test-term-custom");
    PatchEntityInput[] inputArray = {input};

    when(_environment.getArgument("input")).thenReturn(inputArray);

    // Mock entity registry
    setupEntityRegistryMocks();

    // Authorization is already set up by TestUtils.getMockAllowContext()

    // Mock successful batch ingestion
    when(_entityService.ingestProposal(any(), any(), eq(false))).thenReturn(Arrays.asList());

    // Act
    CompletableFuture<List<PatchEntityResult>> future = _resolver.get(_environment);
    List<PatchEntityResult> results = future.get();

    // Assert
    assertNotNull(results);
    assertEquals(results.size(), 1);

    PatchEntityResult result = results.get(0);
    assertNotNull(result);
    assertEquals(result.getUrn(), "urn:li:glossaryTerm:test-term-custom");
    assertEquals(result.getName(), "\"Test Term with Custom Properties\"");
    assertTrue(result.getSuccess());
    assertNull(result.getError());
  }

  @Test
  public void testPatchEntitiesFailure() throws Exception {
    // Arrange - Test batch patch failure
    PatchEntityInput input1 =
        createGlossaryTermInput("urn:li:glossaryTerm:test-term-1", "Test Term 1");
    PatchEntityInput input2 =
        createGlossaryTermInput("urn:li:glossaryTerm:test-term-2", "Test Term 2");
    PatchEntityInput[] inputArray = {input1, input2};

    when(_environment.getArgument("input")).thenReturn(inputArray);

    // Mock entity registry
    setupEntityRegistryMocks();

    // Authorization is already set up by TestUtils.getMockAllowContext()

    // Mock batch ingestion failure
    when(_entityService.ingestProposal(any(), any(), eq(false)))
        .thenThrow(new RuntimeException("Batch ingestion failed"));

    // Act
    CompletableFuture<List<PatchEntityResult>> future = _resolver.get(_environment);
    List<PatchEntityResult> results = future.get();

    // Assert
    assertNotNull(results);
    assertEquals(results.size(), 2);

    // Both results should indicate failure
    for (PatchEntityResult result : results) {
      assertNotNull(result);
      assertFalse(result.getSuccess());
      assertNotNull(result.getError());
      assertTrue(result.getError().contains("Batch ingestion failed"));
    }
  }

  @Test
  public void testPatchEntitiesAuthorizationFailure() throws Exception {
    // Arrange - Test batch patch with authorization failure
    PatchEntityInput input1 =
        createGlossaryTermInput("urn:li:glossaryTerm:test-term-1", "Test Term 1");
    PatchEntityInput input2 =
        createGlossaryTermInput("urn:li:glossaryTerm:test-term-2", "Test Term 2");
    PatchEntityInput[] inputArray = {input1, input2};

    when(_environment.getArgument("input")).thenReturn(inputArray);

    // Mock entity registry
    setupEntityRegistryMocks();

    // Use a deny context for authorization failure
    QueryContext denyContext = TestUtils.getMockDenyContext("urn:li:corpuser:test-user");
    when(_environment.getContext()).thenReturn(denyContext);

    // Act
    CompletableFuture<List<PatchEntityResult>> future = _resolver.get(_environment);
    List<PatchEntityResult> results = future.get();

    // Assert - Authorization failures should result in error results, not exceptions
    assertNotNull(results);
    assertEquals(results.size(), 2);

    // Check first result - should have error due to authorization failure
    PatchEntityResult result1 = results.get(0);
    assertNotNull(result1);
    assertEquals(result1.getUrn(), "urn:li:glossaryTerm:test-term-1");
    assertFalse(result1.getSuccess());
    assertNotNull(result1.getError());
    assertTrue(result1.getError().contains("is unauthorized to update entity"));

    // Check second result - should also have error due to authorization failure
    PatchEntityResult result2 = results.get(1);
    assertNotNull(result2);
    assertEquals(result2.getUrn(), "urn:li:glossaryTerm:test-term-2");
    assertFalse(result2.getSuccess());
    assertNotNull(result2.getError());
    assertTrue(result2.getError().contains("is unauthorized to update entity"));
  }

  @Test
  public void testPatchEntitiesWithMixedEntityTypes() throws Exception {
    // Arrange - Test batch patch with different entity types
    PatchEntityInput glossaryInput =
        createGlossaryTermInput("urn:li:glossaryTerm:test-term", "Test Term");
    PatchEntityInput datasetInput =
        createDatasetInput(
            "urn:li:dataset:(urn:li:dataPlatform:test,dataset,PROD)", "Test Dataset");
    PatchEntityInput[] inputArray = {glossaryInput, datasetInput};

    when(_environment.getArgument("input")).thenReturn(inputArray);

    // Mock entity registry for both entity types
    com.linkedin.metadata.models.EntitySpec glossaryEntitySpec =
        mock(com.linkedin.metadata.models.EntitySpec.class);
    com.linkedin.metadata.models.EntitySpec datasetEntitySpec =
        mock(com.linkedin.metadata.models.EntitySpec.class);
    AspectSpec glossaryAspectSpec = mock(AspectSpec.class);
    AspectSpec datasetAspectSpec = mock(AspectSpec.class);

    when(_entityRegistry.getEntitySpec("glossaryTerm")).thenReturn(glossaryEntitySpec);
    when(_entityRegistry.getEntitySpec("dataset")).thenReturn(datasetEntitySpec);
    when(glossaryEntitySpec.getAspectSpec("glossaryTermInfo")).thenReturn(glossaryAspectSpec);
    when(datasetEntitySpec.getAspectSpec("datasetProperties")).thenReturn(datasetAspectSpec);

    // Authorization is already set up by TestUtils.getMockAllowContext()

    // Mock successful batch ingestion
    when(_entityService.ingestProposal(any(), any(), eq(false))).thenReturn(Arrays.asList());

    // Act
    CompletableFuture<List<PatchEntityResult>> future = _resolver.get(_environment);
    List<PatchEntityResult> results = future.get();

    // Assert
    assertNotNull(results);
    assertEquals(results.size(), 2);

    // Check glossary term result
    PatchEntityResult glossaryResult = results.get(0);
    assertNotNull(glossaryResult);
    assertEquals(glossaryResult.getUrn(), "urn:li:glossaryTerm:test-term");
    assertEquals(glossaryResult.getName(), "\"Test Term\"");
    assertTrue(glossaryResult.getSuccess());
    assertNull(glossaryResult.getError());

    // Check dataset result
    PatchEntityResult datasetResult = results.get(1);
    assertNotNull(datasetResult);
    assertEquals(datasetResult.getUrn(), "urn:li:dataset:(urn:li:dataPlatform:test,dataset,PROD)");
    assertEquals(datasetResult.getName(), "\"Test Dataset\"");
    assertTrue(datasetResult.getSuccess());
    assertNull(datasetResult.getError());
  }

  @Test
  public void testPatchEntitiesEmptyInput() throws Exception {
    // Arrange - Test batch patch with empty input
    PatchEntityInput[] inputArray = {};

    when(_environment.getArgument("input")).thenReturn(inputArray);

    // Act
    CompletableFuture<List<PatchEntityResult>> future = _resolver.get(_environment);
    List<PatchEntityResult> results = future.get();

    // Assert
    assertNotNull(results);
    assertTrue(results.isEmpty());
  }

  @Test
  public void testPatchEntitiesWithComplexPatchOperations() throws Exception {
    // Arrange - Test batch patch with complex patch operations
    PatchEntityInput input = createComplexPatchInput("urn:li:glossaryTerm:complex-term");
    PatchEntityInput[] inputArray = {input};

    when(_environment.getArgument("input")).thenReturn(inputArray);

    // Mock entity registry
    setupEntityRegistryMocks();

    // Authorization is already set up by TestUtils.getMockAllowContext()

    // Mock successful batch ingestion
    when(_entityService.ingestProposal(any(), any(), eq(false))).thenReturn(Arrays.asList());

    // Act
    CompletableFuture<List<PatchEntityResult>> future = _resolver.get(_environment);
    List<PatchEntityResult> results = future.get();

    // Assert
    assertNotNull(results);
    assertEquals(results.size(), 1);

    PatchEntityResult result = results.get(0);
    assertNotNull(result);
    assertEquals(result.getUrn(), "urn:li:glossaryTerm:complex-term");
    assertEquals(result.getName(), "\"Complex Term\"");
    assertTrue(result.getSuccess());
    assertNull(result.getError());
  }

  // Helper methods

  private PatchEntityInput createGlossaryTermInput(String urn, String name) {
    PatchEntityInput input = new PatchEntityInput();
    input.setUrn(urn);
    input.setEntityType("glossaryTerm");
    input.setAspectName("glossaryTermInfo");
    input.setPatch(
        Arrays.asList(
            createPatchOperation(PatchOperationType.ADD, "/name", "\"" + name + "\""),
            createPatchOperation(PatchOperationType.ADD, "/termSource", "\"Internal\""),
            createPatchOperation(PatchOperationType.ADD, "/definition", "\"Test definition\"")));
    return input;
  }

  private PatchEntityInput createGlossaryTermWithCustomProperties(String urn) {
    PatchEntityInput input = new PatchEntityInput();
    input.setUrn(urn);
    input.setEntityType("glossaryTerm");
    input.setAspectName("glossaryTermInfo");
    input.setPatch(
        Arrays.asList(
            createPatchOperation(
                PatchOperationType.ADD, "/name", "\"Test Term with Custom Properties\""),
            createPatchOperation(PatchOperationType.ADD, "/termSource", "\"Internal\""),
            createPatchOperation(PatchOperationType.ADD, "/definition", "\"Test definition\""),
            createPatchOperation(
                PatchOperationType.ADD, "/customProperties/hipaa_compliant", "\"true\""),
            createPatchOperation(
                PatchOperationType.ADD,
                "/customProperties/data_classification",
                "\"confidential\""),
            createPatchOperation(
                PatchOperationType.ADD, "/customProperties/retention_period", "\"7 years\"")));
    return input;
  }

  private PatchEntityInput createDatasetInput(String urn, String name) {
    PatchEntityInput input = new PatchEntityInput();
    input.setUrn(urn);
    input.setEntityType("dataset");
    input.setAspectName("datasetProperties");
    input.setPatch(
        Arrays.asList(
            createPatchOperation(PatchOperationType.ADD, "/name", "\"" + name + "\""),
            createPatchOperation(
                PatchOperationType.ADD, "/description", "\"Test dataset description\"")));
    return input;
  }

  private PatchEntityInput createComplexPatchInput(String urn) {
    PatchEntityInput input = new PatchEntityInput();
    input.setUrn(urn);
    input.setEntityType("glossaryTerm");
    input.setAspectName("glossaryTermInfo");
    input.setPatch(
        Arrays.asList(
            createPatchOperation(PatchOperationType.ADD, "/name", "\"Complex Term\""),
            createPatchOperation(PatchOperationType.ADD, "/termSource", "\"Internal\""),
            createPatchOperation(PatchOperationType.ADD, "/definition", "\"Complex definition\""),
            createPatchOperation(
                PatchOperationType.ADD, "/customProperties/hipaa_compliant", "\"true\""),
            createPatchOperation(
                PatchOperationType.ADD,
                "/customProperties/data_classification",
                "\"confidential\""),
            createPatchOperation(PatchOperationType.REPLACE, "/name", "\"Updated Complex Term\""),
            createPatchOperation(
                PatchOperationType.ADD, "/customProperties/retention_period", "\"10 years\"")));
    return input;
  }

  private PatchOperationInput createPatchOperation(
      PatchOperationType op, String path, String value) {
    PatchOperationInput operation = new PatchOperationInput();
    operation.setOp(op);
    operation.setPath(path);
    operation.setValue(value);
    return operation;
  }

  private void setupEntityRegistryMocks() {
    com.linkedin.metadata.models.EntitySpec mockEntitySpec =
        mock(com.linkedin.metadata.models.EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    when(_entityRegistry.getEntitySpec("glossaryTerm")).thenReturn(mockEntitySpec);
    when(_entityRegistry.getEntitySpec("dataset")).thenReturn(mockEntitySpec);
    when(mockEntitySpec.getAspectSpec("glossaryTermInfo")).thenReturn(mockAspectSpec);
    when(mockEntitySpec.getAspectSpec("datasetProperties")).thenReturn(mockAspectSpec);
  }
}
