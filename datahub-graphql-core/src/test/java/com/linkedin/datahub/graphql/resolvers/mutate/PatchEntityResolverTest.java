package com.linkedin.datahub.graphql.resolvers.mutate;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.PatchEntityInput;
import com.linkedin.datahub.graphql.generated.PatchEntityResult;
import com.linkedin.datahub.graphql.generated.PatchOperationInput;
import com.linkedin.datahub.graphql.generated.PatchOperationType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.mockito.MockedStatic;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PatchEntityResolverTest {

  private EntityService _entityService;
  private EntityClient _entityClient;
  private EntityRegistry _entityRegistry;
  private ObjectMapper _objectMapper;
  private PatchEntityResolver _resolver;
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
    _resolver = new PatchEntityResolver(_entityService, _entityClient, _entityRegistry);

    _environment = mock(DataFetchingEnvironment.class);
    _context = mock(QueryContext.class);
    _authentication = mock(Authentication.class);
    _operationContext = mock(OperationContext.class);

    when(_environment.getContext()).thenReturn(_context);
    when(_context.getAuthentication()).thenReturn(_authentication);
    when(_context.getOperationContext()).thenReturn(_operationContext);
    when(_operationContext.getObjectMapper()).thenReturn(_objectMapper);
    when(_authentication.getActor()).thenReturn(new Actor(ActorType.USER, "test-user"));
  }

  @Test
  public void testPatchEntitySuccess() throws Exception {
    // Arrange
    PatchEntityInput input = new PatchEntityInput();
    input.setUrn("urn:li:glossaryTerm:test-term");
    input.setAspectName("glossaryTermInfo");
    input.setPatch(
        List.of(createPatchOperation(PatchOperationType.REPLACE, "/name", "\"Updated Name\"")));

    when(_environment.getArgument("input")).thenReturn(input);

    IngestResult mockResult = mock(IngestResult.class);
    when(_entityService.ingestProposal(any(), any(), any(), eq(false))).thenReturn(mockResult);

    // Mock entity registry
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    when(_entityRegistry.getEntitySpec("glossaryTerm")).thenReturn(mockEntitySpec);
    when(mockEntitySpec.getAspectSpec("glossaryTermInfo")).thenReturn(mockAspectSpec);

    // Mock authorization to return true
    try (MockedStatic<AuthorizationUtils> mockedAuthUtils = mockStatic(AuthorizationUtils.class)) {
      mockedAuthUtils
          .when(() -> AuthorizationUtils.isAuthorized(any(), any(), any(), any()))
          .thenReturn(true);

      // Act
      CompletableFuture<PatchEntityResult> future = _resolver.get(_environment);
      PatchEntityResult result = future.get();

      // Debug output
      System.out.println("Result: " + result);
      System.out.println("Success: " + result.getSuccess());
      System.out.println("Error: " + result.getError());
      System.out.println("Input URN: " + input.getUrn());
      System.out.println("Input aspectName: " + input.getAspectName());

      // Assert
      assertNotNull(result);
      assertEquals(result.getUrn(), "urn:li:glossaryTerm:test-term");
      assertTrue(
          result.getSuccess(),
          "Expected success=true but got success="
              + result.getSuccess()
              + ", error="
              + result.getError());
      assertNull(result.getError());
    }
  }

  @Test
  public void testPatchEntityAutoGenerateUrn() throws Exception {
    // Arrange - test auto-generation by not providing URN but providing entityType
    PatchEntityInput input = new PatchEntityInput();
    input.setUrn(null); // No URN provided
    input.setEntityType("glossaryTerm"); // But entityType is provided
    input.setAspectName("glossaryTermInfo");
    input.setPatch(
        List.of(createPatchOperation(PatchOperationType.REPLACE, "/name", "\"Updated Name\"")));

    when(_environment.getArgument("input")).thenReturn(input);

    IngestResult mockResult = mock(IngestResult.class);
    when(_entityService.ingestProposal(any(), any(), any(), eq(false))).thenReturn(mockResult);

    // Mock entity registry
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    when(_entityRegistry.getEntitySpec("glossaryTerm")).thenReturn(mockEntitySpec);
    when(mockEntitySpec.getAspectSpec("glossaryTermInfo")).thenReturn(mockAspectSpec);

    // Mock authorization to return true
    try (MockedStatic<AuthorizationUtils> mockedAuthUtils = mockStatic(AuthorizationUtils.class)) {
      mockedAuthUtils
          .when(() -> AuthorizationUtils.isAuthorized(any(), any(), any(), any()))
          .thenReturn(true);

      // Act
      CompletableFuture<PatchEntityResult> future = _resolver.get(_environment);
      PatchEntityResult result = future.get();

      // Debug output
      System.out.println("Auto-generated URN test - Result: " + result);
      System.out.println("Auto-generated URN test - Success: " + result.getSuccess());
      System.out.println("Auto-generated URN test - Error: " + result.getError());
      System.out.println("Auto-generated URN test - Generated URN: " + result.getUrn());

      // Assert
      assertNotNull(result);
      assertTrue(result.getUrn().startsWith("urn:li:glossaryTerm:")); // Should be auto-generated
      assertTrue(
          result.getSuccess(),
          "Expected success=true but got success="
              + result.getSuccess()
              + ", error="
              + result.getError());
      assertNull(result.getError());
    }
  }

  @Test
  public void testPatchEntityFailure() throws Exception {
    // Arrange
    PatchEntityInput input = new PatchEntityInput();
    input.setUrn("urn:li:glossaryTerm:test-term");
    input.setAspectName("glossaryTermInfo");
    input.setPatch(
        List.of(createPatchOperation(PatchOperationType.REPLACE, "/name", "\"Updated Name\"")));

    when(_environment.getArgument("input")).thenReturn(input);

    when(_entityService.ingestProposal(any(), any(), any(), eq(false))).thenReturn(null);

    // Mock entity registry
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    when(_entityRegistry.getEntitySpec("glossaryTerm")).thenReturn(mockEntitySpec);
    when(mockEntitySpec.getAspectSpec("glossaryTermInfo")).thenReturn(mockAspectSpec);

    // Mock authorization to return true
    try (MockedStatic<AuthorizationUtils> mockedAuthUtils = mockStatic(AuthorizationUtils.class)) {
      mockedAuthUtils
          .when(() -> AuthorizationUtils.isAuthorized(any(), any(), any(), any()))
          .thenReturn(true);

      // Act
      CompletableFuture<PatchEntityResult> future = _resolver.get(_environment);
      PatchEntityResult result = future.get();

      // Assert
      assertNotNull(result);
      assertEquals(result.getUrn(), "urn:li:glossaryTerm:test-term");
      assertFalse(result.getSuccess());
      assertNotNull(result.getError());
    }
  }

  private PatchOperationInput createPatchOperation(
      PatchOperationType op, String path, String value) {
    PatchOperationInput operation = new PatchOperationInput();
    operation.setOp(op);
    operation.setPath(path);
    operation.setValue(value);
    return operation;
  }
}
