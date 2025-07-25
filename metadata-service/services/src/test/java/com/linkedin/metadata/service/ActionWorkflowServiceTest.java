package com.linkedin.metadata.service;

import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_STATUS_PENDING;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.actionworkflow.ActionWorkflowCategory;
import com.linkedin.actionworkflow.ActionWorkflowEntrypoint;
import com.linkedin.actionworkflow.ActionWorkflowEntrypointArray;
import com.linkedin.actionworkflow.ActionWorkflowEntrypointType;
import com.linkedin.actionworkflow.ActionWorkflowFieldArray;
import com.linkedin.actionworkflow.ActionWorkflowFormRequest;
import com.linkedin.actionworkflow.ActionWorkflowFormRequestField;
import com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray;
import com.linkedin.actionworkflow.ActionWorkflowInfo;
import com.linkedin.actionworkflow.ActionWorkflowStep;
import com.linkedin.actionworkflow.ActionWorkflowStepActors;
import com.linkedin.actionworkflow.ActionWorkflowStepArray;
import com.linkedin.actionworkflow.ActionWorkflowStepDynamicAssignment;
import com.linkedin.actionworkflow.ActionWorkflowStepDynamicAssignmentType;
import com.linkedin.actionworkflow.ActionWorkflowStepType;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.ActionWorkflowService.ActionWorkflowListResult;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.PropertyCardinality;
import io.datahubproject.metadata.context.AuthorizationContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import java.util.*;
import java.util.Arrays;
import java.util.Collections;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ActionWorkflowServiceTest {

  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final Urn TEST_WORKFLOW_URN =
      UrnUtils.getUrn("urn:li:actionWorkflow:test-workflow");
  private static final Urn TEST_ENTITY_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,SampleDataset,PROD)");
  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:testuser");
  private static final Urn TEST_GROUP_URN = UrnUtils.getUrn("urn:li:corpGroup:testgroup");
  private static final Urn TEST_ROLE_URN = UrnUtils.getUrn("urn:li:dataHubRole:testrole");
  private static final Urn TEST_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:testdomain");
  private static final Urn TEST_DATA_PRODUCT_URN =
      UrnUtils.getUrn("urn:li:dataProduct:testproduct");

  @Mock private SystemEntityClient mockEntityClient;
  @Mock private OpenApiClient mockOpenApiClient;
  @Mock private ObjectMapper mockObjectMapper;
  @Mock private OwnerService mockOwnerService;
  @Mock private DomainService mockDomainService;
  @Mock private DataProductService mockDataProductService;
  @Mock private UserService mockUserService;
  @Mock private OperationContext mockOpContext;
  @Mock private AuthorizationContext mockAuthContext;
  @Mock private Authorizer mockAuthorizer;

  private ActionWorkflowService actionWorkflowService;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    actionWorkflowService =
        new ActionWorkflowService(
            mockEntityClient,
            mockOpenApiClient,
            mockObjectMapper,
            mockOwnerService,
            mockDomainService,
            mockDataProductService,
            mockUserService);

    // Setup common mocks
    when(mockOpContext.getAuthorizationContext()).thenReturn(mockAuthContext);
    when(mockAuthContext.getAuthorizer()).thenReturn(mockAuthorizer);
    Authentication mockAuthentication = mock(Authentication.class);
    when(mockOpContext.getSessionAuthentication()).thenReturn(mockAuthentication);
    when(mockAuthentication.getActor())
        .thenReturn(new Actor(ActorType.USER, TEST_ACTOR_URN.getId()));

    // Provide a default auditStamp
    AuditStamp defaultAuditStamp =
        new AuditStamp().setTime(System.currentTimeMillis()).setActor(TEST_ACTOR_URN);
    when(mockOpContext.getAuditStamp()).thenReturn(defaultAuditStamp);
  }

  /*--------------------------------------------------------------------------
   *             CREATE ACTION WORKFLOW REQUEST TESTS
   *------------------------------------------------------------------------*/

  @Test
  public void testCreateActionWorkflowRequestSuccess() throws Exception {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfo();
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(createMockEntityResponse(workflowInfo));

    List<ActionWorkflowFormRequestField> fields = createTestRequestFields();
    Long expiresAt = System.currentTimeMillis() + 86400000L; // 24 hours

    // Mock owner resolution
    when(mockOwnerService.getEntityOwners(eq(mockOpContext), eq(TEST_ENTITY_URN)))
        .thenReturn(Arrays.asList(createTestOwner(TEST_USER_URN)));

    ArgumentCaptor<Collection<com.linkedin.mxe.MetadataChangeProposal>> proposalsCaptor =
        ArgumentCaptor.forClass(Collection.class);

    // WHEN
    Urn result =
        actionWorkflowService.createActionWorkflowFormRequest(
            TEST_WORKFLOW_URN,
            TEST_ENTITY_URN,
            "Test description",
            fields,
            expiresAt,
            mockOpContext);

    // THEN
    assertNotNull(result);
    verify(mockEntityClient)
        .getV2(eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any());

    // Verify that batchIngestProposals is called with both ActionRequestInfo and
    // ActionRequestStatus aspects
    verify(mockEntityClient, times(1))
        .batchIngestProposals(eq(mockOpContext), proposalsCaptor.capture(), eq(false));

    Collection<com.linkedin.mxe.MetadataChangeProposal> proposals = proposalsCaptor.getValue();
    assertEquals(proposals.size(), 2, "Should batch ingest exactly 2 proposals");

    // Find the ActionRequestInfo and ActionRequestStatus proposals
    boolean foundInfoProposal = false;
    boolean foundStatusProposal = false;

    for (com.linkedin.mxe.MetadataChangeProposal proposal : proposals) {
      if (ACTION_REQUEST_INFO_ASPECT_NAME.equals(proposal.getAspectName())) {
        foundInfoProposal = true;
        assertEquals(
            proposal.getEntityUrn(), result, "Info proposal should have correct entity URN");
      } else if (ACTION_REQUEST_STATUS_ASPECT_NAME.equals(proposal.getAspectName())) {
        foundStatusProposal = true;
        assertEquals(
            proposal.getEntityUrn(), result, "Status proposal should have correct entity URN");

        // Deserialize and validate the ActionRequestStatus aspect
        ActionRequestStatus status =
            GenericRecordUtils.deserializeAspect(
                proposal.getAspect().getValue(),
                proposal.getAspect().getContentType(),
                ActionRequestStatus.class);

        assertEquals(status.getStatus(), ACTION_REQUEST_STATUS_PENDING, "Status should be PENDING");
        assertNotNull(status.getLastModified(), "LastModified should be set");
        assertEquals(
            status.getLastModified().getActor(), TEST_ACTOR_URN, "Actor should be correct");
        assertTrue(status.getLastModified().getTime() > 0, "Time should be positive");
      }
    }

    assertTrue(foundInfoProposal, "ActionRequestInfo aspect should be batched");
    assertTrue(foundStatusProposal, "ActionRequestStatus aspect should be batched");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testCreateActionWorkflowRequestWorkflowNotFound() throws Exception {
    // GIVEN
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(null);

    // WHEN
    actionWorkflowService.createActionWorkflowFormRequest(
        TEST_WORKFLOW_URN,
        TEST_ENTITY_URN,
        "Test description",
        Collections.emptyList(),
        null,
        mockOpContext);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testCreateActionWorkflowRequestNoSteps() throws Exception {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfo();
    workflowInfo.setSteps(new ActionWorkflowStepArray()); // Empty steps
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(createMockEntityResponse(workflowInfo));

    // WHEN
    actionWorkflowService.createActionWorkflowFormRequest(
        TEST_WORKFLOW_URN,
        TEST_ENTITY_URN,
        "Test description",
        Collections.emptyList(),
        null,
        mockOpContext);
  }

  @Test
  public void testCreateActionWorkflowRequestWithDynamicAssignment() throws Exception {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfoWithDynamicAssignment();
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(createMockEntityResponse(workflowInfo));

    List<ActionWorkflowFormRequestField> fields = createTestRequestFields();

    // Mock owner resolution
    when(mockOwnerService.getEntityOwners(eq(mockOpContext), eq(TEST_ENTITY_URN)))
        .thenReturn(Arrays.asList(createTestOwner(TEST_USER_URN)));

    ArgumentCaptor<Collection<com.linkedin.mxe.MetadataChangeProposal>> proposalsCaptor =
        ArgumentCaptor.forClass(Collection.class);

    // WHEN
    Urn result =
        actionWorkflowService.createActionWorkflowFormRequest(
            TEST_WORKFLOW_URN, TEST_ENTITY_URN, "Test description", fields, null, mockOpContext);

    // THEN
    assertNotNull(result);
    verify(mockOwnerService).getEntityOwners(eq(mockOpContext), eq(TEST_ENTITY_URN));

    // Verify that batchIngestProposals is called with both ActionRequestInfo and
    // ActionRequestStatus aspects
    verify(mockEntityClient, times(1))
        .batchIngestProposals(eq(mockOpContext), proposalsCaptor.capture(), eq(false));

    Collection<com.linkedin.mxe.MetadataChangeProposal> proposals = proposalsCaptor.getValue();
    assertEquals(proposals.size(), 2, "Should batch ingest exactly 2 proposals");

    boolean foundInfoProposal = false;
    boolean foundStatusProposal = false;

    for (com.linkedin.mxe.MetadataChangeProposal proposal : proposals) {
      if (ACTION_REQUEST_INFO_ASPECT_NAME.equals(proposal.getAspectName())) {
        foundInfoProposal = true;
      } else if (ACTION_REQUEST_STATUS_ASPECT_NAME.equals(proposal.getAspectName())) {
        foundStatusProposal = true;

        // Deserialize and validate the ActionRequestStatus aspect
        ActionRequestStatus status =
            GenericRecordUtils.deserializeAspect(
                proposal.getAspect().getValue(),
                proposal.getAspect().getContentType(),
                ActionRequestStatus.class);

        assertEquals(status.getStatus(), ACTION_REQUEST_STATUS_PENDING, "Status should be PENDING");
        assertNotNull(status.getLastModified(), "LastModified should be set");
      }
    }

    assertTrue(foundInfoProposal, "ActionRequestInfo aspect should be batched");
    assertTrue(foundStatusProposal, "ActionRequestStatus aspect should be batched");
  }

  @Test
  public void testCreateActionWorkflowRequestWithDomainOwners() throws Exception {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfoWithDomainDynamicAssignment();
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(createMockEntityResponse(workflowInfo));

    List<ActionWorkflowFormRequestField> fields = createTestRequestFields();

    // Mock domain resolution to return a domain
    when(mockDomainService.getEntityDomains(eq(mockOpContext), eq(TEST_ENTITY_URN)))
        .thenReturn(Arrays.asList(TEST_DOMAIN_URN));

    // Mock domain owner resolution
    when(mockOwnerService.getEntityOwners(eq(mockOpContext), eq(TEST_DOMAIN_URN)))
        .thenReturn(Arrays.asList(createTestOwner(TEST_USER_URN)));

    ArgumentCaptor<Collection<com.linkedin.mxe.MetadataChangeProposal>> proposalsCaptor =
        ArgumentCaptor.forClass(Collection.class);

    // WHEN
    Urn result =
        actionWorkflowService.createActionWorkflowFormRequest(
            TEST_WORKFLOW_URN, TEST_ENTITY_URN, "Test description", fields, null, mockOpContext);

    // THEN
    assertNotNull(result);
    verify(mockDomainService).getEntityDomains(eq(mockOpContext), eq(TEST_ENTITY_URN));
    verify(mockOwnerService).getEntityOwners(eq(mockOpContext), eq(TEST_DOMAIN_URN));

    // Verify that batchIngestProposals is called with both ActionRequestInfo and
    // ActionRequestStatus aspects
    verify(mockEntityClient, times(1))
        .batchIngestProposals(eq(mockOpContext), proposalsCaptor.capture(), eq(false));

    Collection<com.linkedin.mxe.MetadataChangeProposal> proposals = proposalsCaptor.getValue();
    assertEquals(proposals.size(), 2, "Should batch ingest exactly 2 proposals");

    boolean foundInfoProposal = false;
    boolean foundStatusProposal = false;

    for (com.linkedin.mxe.MetadataChangeProposal proposal : proposals) {
      if (ACTION_REQUEST_INFO_ASPECT_NAME.equals(proposal.getAspectName())) {
        foundInfoProposal = true;
      } else if (ACTION_REQUEST_STATUS_ASPECT_NAME.equals(proposal.getAspectName())) {
        foundStatusProposal = true;

        // Deserialize and validate the ActionRequestStatus aspect
        ActionRequestStatus status =
            GenericRecordUtils.deserializeAspect(
                proposal.getAspect().getValue(),
                proposal.getAspect().getContentType(),
                ActionRequestStatus.class);

        assertEquals(status.getStatus(), ACTION_REQUEST_STATUS_PENDING, "Status should be PENDING");
        assertNotNull(status.getLastModified(), "LastModified should be set");
      }
    }

    assertTrue(foundInfoProposal, "ActionRequestInfo aspect should be batched");
    assertTrue(foundStatusProposal, "ActionRequestStatus aspect should be batched");
  }

  @Test
  public void testCreateActionWorkflowRequestWithDataProductOwners() throws Exception {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfoWithDataProductDynamicAssignment();
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(createMockEntityResponse(workflowInfo));

    List<ActionWorkflowFormRequestField> fields = createTestRequestFields();

    // Mock data product resolution to return a data product
    when(mockDataProductService.getDataProductsContainingEntity(
            eq(mockOpContext), eq(TEST_ENTITY_URN)))
        .thenReturn(Arrays.asList(TEST_DATA_PRODUCT_URN));

    // Mock data product owner resolution
    when(mockOwnerService.getEntityOwners(eq(mockOpContext), eq(TEST_DATA_PRODUCT_URN)))
        .thenReturn(Arrays.asList(createTestOwner(TEST_USER_URN)));

    ArgumentCaptor<Collection<com.linkedin.mxe.MetadataChangeProposal>> proposalsCaptor =
        ArgumentCaptor.forClass(Collection.class);

    // WHEN
    Urn result =
        actionWorkflowService.createActionWorkflowFormRequest(
            TEST_WORKFLOW_URN, TEST_ENTITY_URN, "Test description", fields, null, mockOpContext);

    // THEN
    assertNotNull(result);
    verify(mockDataProductService)
        .getDataProductsContainingEntity(eq(mockOpContext), eq(TEST_ENTITY_URN));
    verify(mockOwnerService).getEntityOwners(eq(mockOpContext), eq(TEST_DATA_PRODUCT_URN));

    // Verify that batchIngestProposals is called with both ActionRequestInfo and
    // ActionRequestStatus aspects
    verify(mockEntityClient, times(1))
        .batchIngestProposals(eq(mockOpContext), proposalsCaptor.capture(), eq(false));

    Collection<com.linkedin.mxe.MetadataChangeProposal> proposals = proposalsCaptor.getValue();
    assertEquals(proposals.size(), 2, "Should batch ingest exactly 2 proposals");

    boolean foundInfoProposal = false;
    boolean foundStatusProposal = false;

    for (com.linkedin.mxe.MetadataChangeProposal proposal : proposals) {
      if (ACTION_REQUEST_INFO_ASPECT_NAME.equals(proposal.getAspectName())) {
        foundInfoProposal = true;
      } else if (ACTION_REQUEST_STATUS_ASPECT_NAME.equals(proposal.getAspectName())) {
        foundStatusProposal = true;

        // Deserialize and validate the ActionRequestStatus aspect
        ActionRequestStatus status =
            GenericRecordUtils.deserializeAspect(
                proposal.getAspect().getValue(),
                proposal.getAspect().getContentType(),
                ActionRequestStatus.class);

        assertEquals(status.getStatus(), ACTION_REQUEST_STATUS_PENDING, "Status should be PENDING");
        assertNotNull(status.getLastModified(), "LastModified should be set");
      }
    }

    assertTrue(foundInfoProposal, "ActionRequestInfo aspect should be batched");
    assertTrue(foundStatusProposal, "ActionRequestStatus aspect should be batched");
  }

  @Test
  public void testCreateActionWorkflowRequestEmitsPendingStatus() throws Exception {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfo();
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(createMockEntityResponse(workflowInfo));

    List<ActionWorkflowFormRequestField> fields = createTestRequestFields();

    // Mock owner resolution
    when(mockOwnerService.getEntityOwners(eq(mockOpContext), eq(TEST_ENTITY_URN)))
        .thenReturn(Arrays.asList(createTestOwner(TEST_USER_URN)));

    ArgumentCaptor<Collection<com.linkedin.mxe.MetadataChangeProposal>> proposalsCaptor =
        ArgumentCaptor.forClass(Collection.class);

    // WHEN
    Urn result =
        actionWorkflowService.createActionWorkflowFormRequest(
            TEST_WORKFLOW_URN, TEST_ENTITY_URN, "Test description", fields, null, mockOpContext);

    // THEN
    assertNotNull(result);
    verify(mockEntityClient, times(1))
        .batchIngestProposals(eq(mockOpContext), proposalsCaptor.capture(), eq(false));

    Collection<com.linkedin.mxe.MetadataChangeProposal> proposals = proposalsCaptor.getValue();
    assertEquals(proposals.size(), 2, "Should batch ingest exactly 2 proposals");

    // Find and validate the ActionRequestStatus proposal
    com.linkedin.mxe.MetadataChangeProposal statusProposal =
        proposals.stream()
            .filter(proposal -> ACTION_REQUEST_STATUS_ASPECT_NAME.equals(proposal.getAspectName()))
            .findFirst()
            .orElse(null);

    assertNotNull(statusProposal, "ActionRequestStatus proposal should be present");
    assertEquals(
        statusProposal.getEntityUrn(), result, "Status proposal should have correct entity URN");
    assertEquals(
        statusProposal.getAspectName(),
        ACTION_REQUEST_STATUS_ASPECT_NAME,
        "Status proposal should have correct aspect name");

    // Deserialize and thoroughly validate the ActionRequestStatus
    ActionRequestStatus status =
        GenericRecordUtils.deserializeAspect(
            statusProposal.getAspect().getValue(),
            statusProposal.getAspect().getContentType(),
            ActionRequestStatus.class);

    // Validate the status field
    assertEquals(status.getStatus(), ACTION_REQUEST_STATUS_PENDING, "Status should be PENDING");

    // Validate the lastModified audit stamp
    assertNotNull(status.getLastModified(), "LastModified audit stamp should be present");
    assertEquals(
        status.getLastModified().getActor(),
        TEST_ACTOR_URN,
        "LastModified actor should match session actor");
    assertTrue(
        status.getLastModified().getTime() > 0, "LastModified time should be positive timestamp");

    // Validate that result and note are not set (should be null for initial status)
    assertFalse(status.hasResult(), "Result should not be set for initial status");
    assertFalse(status.hasNote(), "Note should not be set for initial status");
  }

  /*--------------------------------------------------------------------------
   *             UPSERT ACTION WORKFLOW TESTS
   *------------------------------------------------------------------------*/

  @Test
  public void testUpsertActionWorkflowCreate() throws Exception {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfo();

    // WHEN
    Urn result = actionWorkflowService.upsertActionWorkflow(mockOpContext, null, workflowInfo);

    // THEN
    assertNotNull(result);
    verify(mockEntityClient).ingestProposal(eq(mockOpContext), any(), eq(false));
  }

  @Test
  public void testUpsertActionWorkflowUpdate() throws Exception {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfo();

    // WHEN
    Urn result =
        actionWorkflowService.upsertActionWorkflow(mockOpContext, TEST_WORKFLOW_URN, workflowInfo);

    // THEN
    assertNotNull(result);
    verify(mockEntityClient).ingestProposal(eq(mockOpContext), any(), eq(false));
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testUpsertActionWorkflowFailure() throws Exception {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfo();
    when(mockEntityClient.ingestProposal(eq(mockOpContext), any(), eq(false)))
        .thenThrow(new RuntimeException("Ingestion failed"));

    // WHEN
    actionWorkflowService.upsertActionWorkflow(mockOpContext, null, workflowInfo);
  }

  /*--------------------------------------------------------------------------
   *             DELETE ACTION WORKFLOW TESTS
   *------------------------------------------------------------------------*/

  @Test
  public void testDeleteActionWorkflowSuccess() throws Exception {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfo();
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(createMockEntityResponse(workflowInfo));

    // WHEN
    actionWorkflowService.deleteActionWorkflow(mockOpContext, TEST_WORKFLOW_URN);

    // THEN
    verify(mockEntityClient, times(1))
        .getV2(eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any());
    verify(mockEntityClient, times(1)).deleteEntity(eq(mockOpContext), eq(TEST_WORKFLOW_URN));
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testDeleteActionWorkflowNotFound() throws Exception {
    // GIVEN
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(null);

    // WHEN
    actionWorkflowService.deleteActionWorkflow(mockOpContext, TEST_WORKFLOW_URN);

    // THEN - should throw RuntimeException from getActionWorkflow
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testDeleteActionWorkflowMissingAspect() throws Exception {
    // GIVEN
    EntityResponse mockResponse = mock(EntityResponse.class);
    when(mockResponse.getAspects()).thenReturn(new EnvelopedAspectMap());
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(mockResponse);

    // WHEN
    actionWorkflowService.deleteActionWorkflow(mockOpContext, TEST_WORKFLOW_URN);

    // THEN - should throw RuntimeException from getActionWorkflow
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testDeleteActionWorkflowDeleteFailure() throws Exception {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfo();
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(createMockEntityResponse(workflowInfo));

    doThrow(new RuntimeException("Delete failed"))
        .when(mockEntityClient)
        .deleteEntity(eq(mockOpContext), eq(TEST_WORKFLOW_URN));

    // WHEN
    actionWorkflowService.deleteActionWorkflow(mockOpContext, TEST_WORKFLOW_URN);

    // THEN - should throw RuntimeException from deleteEntity
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testDeleteActionWorkflowNullOpContext() throws Exception {
    // WHEN
    actionWorkflowService.deleteActionWorkflow(null, TEST_WORKFLOW_URN);

    // THEN - should throw NullPointerException
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testDeleteActionWorkflowNullUrn() throws Exception {
    // WHEN
    actionWorkflowService.deleteActionWorkflow(mockOpContext, null);

    // THEN - should throw NullPointerException
  }

  /*--------------------------------------------------------------------------
   *             GET ACTION WORKFLOW TESTS
   *------------------------------------------------------------------------*/

  @Test
  public void testGetActionWorkflowSuccess() throws Exception {
    // GIVEN
    ActionWorkflowInfo expectedWorkflowInfo = createTestWorkflowInfo();
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(createMockEntityResponse(expectedWorkflowInfo));

    // WHEN
    ActionWorkflowInfo result =
        actionWorkflowService.getActionWorkflow(mockOpContext, TEST_WORKFLOW_URN);

    // THEN
    assertNotNull(result);
    assertEquals(result.getName(), expectedWorkflowInfo.getName());
    assertEquals(result.getCategory(), expectedWorkflowInfo.getCategory());
    verify(mockEntityClient)
        .getV2(eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any());
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetActionWorkflowNotFound() throws Exception {
    // GIVEN
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(null);

    // WHEN
    actionWorkflowService.getActionWorkflow(mockOpContext, TEST_WORKFLOW_URN);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetActionWorkflowMissingAspect() throws Exception {
    // GIVEN
    EntityResponse mockResponse = mock(EntityResponse.class);
    when(mockResponse.getAspects()).thenReturn(new EnvelopedAspectMap());
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(mockResponse);

    // WHEN
    actionWorkflowService.getActionWorkflow(mockOpContext, TEST_WORKFLOW_URN);
  }

  /*--------------------------------------------------------------------------
   *             LIST ACTION WORKFLOWS TESTS
   *------------------------------------------------------------------------*/

  @Test
  public void testListActionWorkflowsSuccess() throws Exception {
    // GIVEN
    when(mockEntityClient.search(
            eq(mockOpContext),
            eq(ACTION_WORKFLOW_ENTITY_NAME),
            eq("*"),
            any(),
            any(),
            eq(0),
            eq(10)))
        .thenReturn(createMockSearchResult());

    ActionWorkflowInfo workflowInfo = createTestWorkflowInfo();
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(createMockEntityResponse(workflowInfo));
    ActionWorkflowListResult result =
        actionWorkflowService.listActionWorkflows(mockOpContext, 0, 10, null, null, null, null);

    // THEN
    assertNotNull(result);
    assertNotNull(result.getWorkflows());
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 1);
    assertEquals(result.getTotal(), 1);
  }

  @Test
  public void testListActionWorkflowsWithFilters() throws Exception {
    // GIVEN
    when(mockEntityClient.search(
            eq(mockOpContext),
            eq(ACTION_WORKFLOW_ENTITY_NAME),
            eq("*"),
            any(),
            any(),
            eq(0),
            eq(10)))
        .thenReturn(createMockSearchResult());

    ActionWorkflowInfo workflowInfo = createTestWorkflowInfo();
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(createMockEntityResponse(workflowInfo));

    ActionWorkflowListResult result =
        actionWorkflowService.listActionWorkflows(
            mockOpContext,
            0,
            10,
            ActionWorkflowCategory.ACCESS,
            "custom-category",
            "ENTITY_PROFILE",
            "dataset");

    // THEN
    assertNotNull(result);
    assertNotNull(result.getWorkflows());
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 1);
    assertEquals(result.getTotal(), 1);
  }

  @Test
  public void testListActionWorkflowsDefaults() throws Exception {
    // GIVEN
    when(mockEntityClient.search(
            eq(mockOpContext),
            eq(ACTION_WORKFLOW_ENTITY_NAME),
            eq("*"),
            any(),
            any(),
            eq(0),
            eq(20)))
        .thenReturn(createMockSearchResult());

    ActionWorkflowInfo workflowInfo = createTestWorkflowInfo();
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(createMockEntityResponse(workflowInfo));

    ActionWorkflowListResult result =
        actionWorkflowService.listActionWorkflows(
            mockOpContext, null, null, null, null, null, null);

    // THEN
    assertNotNull(result);
    assertNotNull(result.getWorkflows());
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 1);
    assertEquals(result.getTotal(), 1);
  }

  @Test
  public void testListActionWorkflowsEmpty() throws Exception {
    // GIVEN
    when(mockEntityClient.search(
            eq(mockOpContext),
            eq(ACTION_WORKFLOW_ENTITY_NAME),
            eq("*"),
            any(),
            any(),
            eq(0),
            eq(10)))
        .thenReturn(createEmptySearchResult());

    // WHEN
    ActionWorkflowListResult result =
        actionWorkflowService.listActionWorkflows(mockOpContext, 0, 10, null, null, null, null);

    // THEN
    assertNotNull(result);
    assertEquals(result.getWorkflows().size(), 0);
    assertEquals(result.getTotal(), 0);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testListActionWorkflowsSearchFailure() throws Exception {
    // GIVEN
    when(mockEntityClient.search(
            eq(mockOpContext),
            eq(ACTION_WORKFLOW_ENTITY_NAME),
            eq("*"),
            any(),
            any(),
            eq(0),
            eq(10)))
        .thenThrow(new RuntimeException("Search failed"));

    // WHEN
    actionWorkflowService.listActionWorkflows(mockOpContext, 0, 10, null, null, null, null);
  }

  /*--------------------------------------------------------------------------
   *             REVIEW ACTION WORKFLOW REQUEST TESTS
   *------------------------------------------------------------------------*/

  @Test
  public void testReviewActionWorkflowRequestApprovalSuccess() throws Exception {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfo();
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(createMockEntityResponse(workflowInfo));

    ActionRequestInfo actionRequestInfo = createTestActionRequestInfo();
    when(mockEntityClient.getV2(eq(mockOpContext), eq(ACTION_REQUEST_ENTITY_NAME), any(), any()))
        .thenReturn(createMockActionRequestEntityResponse(actionRequestInfo));

    // WHEN
    boolean result =
        actionWorkflowService.reviewActionWorkflowFormRequest(
            UrnUtils.getUrn("urn:li:actionRequest:test-request"),
            TEST_USER_URN,
            "ACCEPTED",
            "Approved",
            mockOpContext);

    // THEN
    assertTrue(result);
    verify(mockEntityClient, times(1)).ingestProposal(eq(mockOpContext), any(), eq(false));
  }

  @Test
  public void testReviewActionWorkflowRequestRejectionSuccess() throws Exception {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfo();
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(createMockEntityResponse(workflowInfo));

    ActionRequestInfo actionRequestInfo = createTestActionRequestInfo();
    when(mockEntityClient.getV2(eq(mockOpContext), eq(ACTION_REQUEST_ENTITY_NAME), any(), any()))
        .thenReturn(createMockActionRequestEntityResponse(actionRequestInfo));

    // WHEN
    boolean result =
        actionWorkflowService.reviewActionWorkflowFormRequest(
            UrnUtils.getUrn("urn:li:actionRequest:test-request"),
            TEST_USER_URN,
            "REJECTED",
            "Rejected",
            mockOpContext);

    // THEN
    assertTrue(result);
    verify(mockEntityClient, times(1)).ingestProposal(eq(mockOpContext), any(), eq(false));
  }

  @Test
  public void testReviewActionWorkflowRequestFinalStepApproval() throws Exception {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfo();
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(createMockEntityResponse(workflowInfo));

    ActionRequestInfo actionRequestInfo = createTestActionRequestInfoWithFinalStep();
    when(mockEntityClient.getV2(eq(mockOpContext), eq(ACTION_REQUEST_ENTITY_NAME), any(), any()))
        .thenReturn(createMockActionRequestEntityResponse(actionRequestInfo));

    // WHEN
    boolean result =
        actionWorkflowService.reviewActionWorkflowFormRequest(
            UrnUtils.getUrn("urn:li:actionRequest:test-request"),
            TEST_USER_URN,
            "ACCEPTED",
            "Approved",
            mockOpContext);

    // THEN
    assertTrue(result);
    verify(mockEntityClient, times(1)).ingestProposal(eq(mockOpContext), any(), eq(false));
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*not authorized.*")
  public void testReviewActionWorkflowRequestUnauthorized() throws Exception {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfo();
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(createMockEntityResponse(workflowInfo));

    ActionRequestInfo actionRequestInfo = createTestActionRequestInfo();
    when(mockEntityClient.getV2(eq(mockOpContext), eq(ACTION_REQUEST_ENTITY_NAME), any(), any()))
        .thenReturn(createMockActionRequestEntityResponse(actionRequestInfo));

    // WHEN
    actionWorkflowService.reviewActionWorkflowFormRequest(
        UrnUtils.getUrn("urn:li:actionRequest:test-request"),
        UrnUtils.getUrn("urn:li:corpuser:unauthorized"),
        "ACCEPTED",
        "Approved",
        mockOpContext);
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*Failed to find action request.*")
  public void testReviewActionWorkflowRequestNotFound() throws Exception {
    // GIVEN
    when(mockEntityClient.getV2(eq(mockOpContext), eq(ACTION_REQUEST_ENTITY_NAME), any(), any()))
        .thenReturn(null);

    // WHEN
    actionWorkflowService.reviewActionWorkflowFormRequest(
        UrnUtils.getUrn("urn:li:actionRequest:test-request"),
        TEST_USER_URN,
        "ACCEPTED",
        "Approved",
        mockOpContext);
  }

  @Test
  public void testReviewActionWorkflowRequestNoWorkflowRequest() throws Exception {
    // GIVEN
    ActionRequestInfo actionRequestInfo = createTestActionRequestInfoWithoutWorkflowRequest();
    when(mockEntityClient.getV2(eq(mockOpContext), eq(ACTION_REQUEST_ENTITY_NAME), any(), any()))
        .thenReturn(createMockActionRequestEntityResponse(actionRequestInfo));

    // WHEN
    boolean result =
        actionWorkflowService.reviewActionWorkflowFormRequest(
            UrnUtils.getUrn("urn:li:actionRequest:test-request"),
            TEST_USER_URN,
            "ACCEPTED",
            "Approved",
            mockOpContext);

    // THEN
    assertTrue(result);
    verify(mockEntityClient, times(1)).ingestProposal(eq(mockOpContext), any(), eq(false));
  }

  @Test
  public void testReviewActionWorkflowRequestDirectUserAssignment() throws Exception {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfo();
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(createMockEntityResponse(workflowInfo));

    ActionRequestInfo actionRequestInfo = createTestActionRequestInfoWithDirectUserAssignment();
    when(mockEntityClient.getV2(eq(mockOpContext), eq(ACTION_REQUEST_ENTITY_NAME), any(), any()))
        .thenReturn(createMockActionRequestEntityResponse(actionRequestInfo));

    // WHEN
    boolean result =
        actionWorkflowService.reviewActionWorkflowFormRequest(
            UrnUtils.getUrn("urn:li:actionRequest:test-request"),
            TEST_USER_URN,
            "ACCEPTED",
            "Approved",
            mockOpContext);

    // THEN
    assertTrue(result);
    verify(mockEntityClient, times(1)).ingestProposal(eq(mockOpContext), any(), eq(false));
  }

  @Test
  public void testReviewActionWorkflowRequestEmitsCorrectStatusAspect() throws Exception {
    // GIVEN
    ActionRequestInfo actionRequestInfo = createTestActionRequestInfo();
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_REQUEST_ENTITY_NAME), any(Urn.class), any()))
        .thenReturn(createMockActionRequestEntityResponse(actionRequestInfo));

    ActionWorkflowInfo workflowInfo = createTestWorkflowInfo();
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(createMockEntityResponse(workflowInfo));

    // Mock owner resolution for the next step
    when(mockOwnerService.getEntityOwners(eq(mockOpContext), any(Urn.class)))
        .thenReturn(Arrays.asList(createTestOwner(TEST_USER_URN)));

    ArgumentCaptor<com.linkedin.mxe.MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(com.linkedin.mxe.MetadataChangeProposal.class);

    // WHEN
    actionWorkflowService.reviewActionWorkflowFormRequest(
        UrnUtils.getUrn("urn:li:actionRequest:test"),
        TEST_USER_URN,
        "ACCEPTED",
        "Approved",
        mockOpContext);

    // THEN
    verify(mockEntityClient, atLeast(1))
        .ingestProposal(eq(mockOpContext), proposalCaptor.capture(), eq(false));

    // Verify that the status aspect was emitted
    List<com.linkedin.mxe.MetadataChangeProposal> proposals = proposalCaptor.getAllValues();
    boolean statusProposalFound = false;
    for (com.linkedin.mxe.MetadataChangeProposal proposal : proposals) {
      if (ACTION_REQUEST_STATUS_ASPECT_NAME.equals(proposal.getAspectName())) {
        statusProposalFound = true;
        break;
      }
    }
    assertTrue(statusProposalFound, "ActionRequestStatus aspect should be updated");
  }

  /*--------------------------------------------------------------------------
   *                      VALIDATION TESTS
   *------------------------------------------------------------------------*/

  @Test
  public void testValidateWorkflowRequestValidCase() {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfoWithoutFields();
    ActionWorkflowFormRequest workflowRequest = createEmptyFieldsWorkflowRequest();

    // WHEN & THEN - should not throw any exception
    ActionWorkflowService.validateWorkflowFormRequestAgainstDefinition(
        workflowRequest, workflowInfo);
  }

  @Test(
      expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*must have at least one field.*")
  public void testValidateWorkflowRequestEmptyFieldsWhenRequired() {
    // GIVEN
    ActionWorkflowInfo workflowInfo =
        createTestWorkflowInfoWithMultipleFields(); // Has required fields - changed this
    ActionWorkflowFormRequest workflowRequest = createEmptyFieldsWorkflowRequest();

    // WHEN & THEN
    ActionWorkflowService.validateWorkflowFormRequestAgainstDefinition(
        workflowRequest, workflowInfo);
  }

  @Test
  public void testValidateWorkflowRequestEmptyFieldsWhenNotRequired() {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfoWithoutFields(); // No fields required
    ActionWorkflowFormRequest workflowRequest = createEmptyFieldsWorkflowRequest();

    // WHEN & THEN - should not throw any exception
    ActionWorkflowService.validateWorkflowFormRequestAgainstDefinition(
        workflowRequest, workflowInfo);
  }

  @Test(
      expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*not defined in workflow.*")
  public void testValidateWorkflowRequestInvalidFieldId() {
    // GIVEN
    ActionWorkflowInfo workflowInfo =
        createTestWorkflowInfoWithMultipleFields(); // Has required_field and optional_field
    ActionWorkflowFormRequest workflowRequest = createWorkflowRequestWithInvalidField();

    // WHEN & THEN
    ActionWorkflowService.validateWorkflowFormRequestAgainstDefinition(
        workflowRequest, workflowInfo);
  }

  @Test(
      expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*missing from workflow request.*")
  public void testValidateWorkflowRequestMissingRequiredField() {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfoWithMultipleFields();
    ActionWorkflowFormRequest workflowRequest = createWorkflowRequestMissingRequiredField();

    // WHEN & THEN
    ActionWorkflowService.validateWorkflowFormRequestAgainstDefinition(
        workflowRequest, workflowInfo);
  }

  @Test(
      expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*accepts only one value.*")
  public void testValidateWorkflowRequestWrongCardinality() {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfoWithSingleCardinalityField();
    ActionWorkflowFormRequest workflowRequest = createWorkflowRequestWithMultipleValues();

    // WHEN & THEN
    ActionWorkflowService.validateWorkflowFormRequestAgainstDefinition(
        workflowRequest, workflowInfo);
  }

  @Test(
      expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*must have at least one value.*")
  public void testValidateWorkflowRequestEmptyValuesForRequiredField() {
    // GIVEN
    ActionWorkflowInfo workflowInfo =
        createTestWorkflowInfoWithMultipleFields(); // Has required_field
    ActionWorkflowFormRequest workflowRequest =
        createWorkflowRequestWithEmptyFieldValuesForRequiredField();

    // WHEN & THEN
    ActionWorkflowService.validateWorkflowFormRequestAgainstDefinition(
        workflowRequest, workflowInfo);
  }

  @Test(
      expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*not in the list of allowed values.*")
  public void testValidateWorkflowRequestInvalidAllowedValueString() {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfoWithAllowedValues();
    ActionWorkflowFormRequest workflowRequest = createWorkflowRequestWithInvalidStringValue();

    // WHEN & THEN
    ActionWorkflowService.validateWorkflowFormRequestAgainstDefinition(
        workflowRequest, workflowInfo);
  }

  @Test(
      expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*not in the list of allowed values.*")
  public void testValidateWorkflowRequestInvalidAllowedValueNumber() {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfoWithAllowedNumberValues();
    ActionWorkflowFormRequest workflowRequest = createWorkflowRequestWithInvalidNumberValue();

    // WHEN & THEN
    ActionWorkflowService.validateWorkflowFormRequestAgainstDefinition(
        workflowRequest, workflowInfo);
  }

  @Test
  public void testValidateWorkflowRequestValidAllowedValues() {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfoWithAllowedValues();
    ActionWorkflowFormRequest workflowRequest = createWorkflowRequestWithValidAllowedValue();

    // WHEN & THEN - should not throw any exception
    ActionWorkflowService.validateWorkflowFormRequestAgainstDefinition(
        workflowRequest, workflowInfo);
  }

  @Test
  public void testValidateWorkflowRequestMultipleFieldsValid() {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfoWithMultipleFields();
    ActionWorkflowFormRequest workflowRequest = createWorkflowRequestWithMultipleFieldsValid();

    // WHEN & THEN - should not throw any exception
    ActionWorkflowService.validateWorkflowFormRequestAgainstDefinition(
        workflowRequest, workflowInfo);
  }

  /*--------------------------------------------------------------------------
   *                      CONDITIONAL VALIDATION TESTS
   *------------------------------------------------------------------------*/

  @Test
  public void testValidateWorkflowRequestConditionalFieldHiddenShouldPass() {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfoWithConditionalField();
    // Request provides trigger field but not the conditional field (which should be hidden)
    ActionWorkflowFormRequest workflowRequest = createWorkflowRequestWithTriggerFieldOnly();

    // WHEN & THEN - should not throw any exception since conditional field is hidden
    ActionWorkflowService.validateWorkflowFormRequestAgainstDefinition(
        workflowRequest, workflowInfo);
  }

  @Test(
      expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp =
          ".*Required field 'conditional_field' is missing from workflow request.*")
  public void testValidateWorkflowRequestConditionalFieldVisibleButMissing() {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfoWithConditionalField();
    // Request provides trigger field with value that makes conditional field visible
    ActionWorkflowFormRequest workflowRequest = createWorkflowRequestTriggeringConditionalField();

    // WHEN & THEN - should throw exception since conditional field is visible but missing
    ActionWorkflowService.validateWorkflowFormRequestAgainstDefinition(
        workflowRequest, workflowInfo);
  }

  @Test
  public void testValidateWorkflowRequestConditionalFieldVisibleAndProvided() {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfoWithConditionalField();
    // Request provides both trigger field and conditional field
    ActionWorkflowFormRequest workflowRequest = createWorkflowRequestWithBothFields();

    // WHEN & THEN - should not throw any exception
    ActionWorkflowService.validateWorkflowFormRequestAgainstDefinition(
        workflowRequest, workflowInfo);
  }

  @Test
  public void testValidateWorkflowRequestConditionalFieldNegatedCondition() {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfoWithNegatedConditionalField();
    // Request provides trigger field with value that should hide the conditional field (due to
    // negation)
    ActionWorkflowFormRequest workflowRequest = createWorkflowRequestWithNegatedConditionValue();

    // WHEN & THEN - should not throw any exception since conditional field is hidden due to
    // negation
    ActionWorkflowService.validateWorkflowFormRequestAgainstDefinition(
        workflowRequest, workflowInfo);
  }

  @Test
  public void testValidateWorkflowRequestConditionalFieldContainOperator() {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfoWithContainCondition();
    // Request provides trigger field with value containing the condition string
    ActionWorkflowFormRequest workflowRequest = createWorkflowRequestWithContainValue();

    // WHEN & THEN - should not throw any exception since conditional field should be visible
    ActionWorkflowService.validateWorkflowFormRequestAgainstDefinition(
        workflowRequest, workflowInfo);
  }

  @Test
  public void testValidateWorkflowRequestConditionalFieldMultipleValues() {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfoWithMultipleConditionValues();
    // Request provides trigger field with one of the multiple allowed values
    ActionWorkflowFormRequest workflowRequest = createWorkflowRequestWithOneOfMultipleValues();

    // WHEN & THEN - should not throw any exception since conditional field should be visible
    ActionWorkflowService.validateWorkflowFormRequestAgainstDefinition(
        workflowRequest, workflowInfo);
  }

  @Test
  public void testValidateWorkflowRequestConditionalFieldHiddenButProvidedWithEmptyValues() {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfoWithConditionalField();
    // Request provides trigger field that hides conditional field, AND provides conditional field
    // with empty values
    ActionWorkflowFormRequest workflowRequest =
        createWorkflowRequestWithHiddenFieldButEmptyValues();

    // WHEN & THEN - should NOT throw exception even though required field has empty values,
    // because the field should be hidden and therefore not required
    ActionWorkflowService.validateWorkflowFormRequestAgainstDefinition(
        workflowRequest, workflowInfo);
  }

  @Test
  public void testValidateWorkflowRequestConditionalFieldHiddenWithNonEmptyValues() {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfoWithConditionalField();
    // Request provides trigger field that hides conditional field, but also provides conditional
    // field with values
    ActionWorkflowFormRequest workflowRequest = createWorkflowRequestWithHiddenFieldButWithValues();

    // WHEN & THEN - should NOT throw exception. Hidden fields can have values if provided.
    // This tests that validation doesn't reject hidden fields that happen to have values.
    ActionWorkflowService.validateWorkflowFormRequestAgainstDefinition(
        workflowRequest, workflowInfo);
  }

  /*--------------------------------------------------------------------------
   *                      HELPER METHODS FOR CONDITIONAL VALIDATION TESTS
   *------------------------------------------------------------------------*/

  private ActionWorkflowInfo createTestWorkflowInfoWithConditionalField() {
    ActionWorkflowInfo workflowInfo = new ActionWorkflowInfo();
    workflowInfo.setName("Test Conditional Workflow");
    workflowInfo.setCategory(ActionWorkflowCategory.ACCESS);

    // Create trigger with form
    com.linkedin.actionworkflow.ActionWorkflowTrigger trigger =
        new com.linkedin.actionworkflow.ActionWorkflowTrigger();
    trigger.setType(com.linkedin.actionworkflow.ActionWorkflowTriggerType.FORM_SUBMITTED);

    com.linkedin.actionworkflow.ActionWorkflowForm form =
        new com.linkedin.actionworkflow.ActionWorkflowForm();

    // Create trigger field (always visible)
    com.linkedin.actionworkflow.ActionWorkflowField triggerField =
        new com.linkedin.actionworkflow.ActionWorkflowField();
    triggerField.setId("trigger_field");
    triggerField.setName("Trigger Field");
    triggerField.setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"));
    triggerField.setCardinality(PropertyCardinality.SINGLE);
    triggerField.setRequired(true);

    // Create conditional field (visible when trigger_field equals "SHOW")
    com.linkedin.actionworkflow.ActionWorkflowField conditionalField =
        new com.linkedin.actionworkflow.ActionWorkflowField();
    conditionalField.setId("conditional_field");
    conditionalField.setName("Conditional Field");
    conditionalField.setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"));
    conditionalField.setCardinality(PropertyCardinality.SINGLE);
    conditionalField.setRequired(true);

    // Create condition
    com.linkedin.actionworkflow.ActionWorkflowFieldCondition condition =
        new com.linkedin.actionworkflow.ActionWorkflowFieldCondition();
    condition.setType(
        com.linkedin.actionworkflow.ActionWorkflowFieldConditionType.SINGLE_FIELD_VALUE);

    com.linkedin.actionworkflow.ActionWorkflowSingleFieldValueCondition singleCondition =
        new com.linkedin.actionworkflow.ActionWorkflowSingleFieldValueCondition();
    singleCondition.setField("trigger_field");
    singleCondition.setValues(new StringArray(Collections.singletonList("SHOW")));
    singleCondition.setCondition(Condition.EQUAL);
    singleCondition.setNegated(false);

    condition.setSingleFieldValueCondition(singleCondition);
    conditionalField.setCondition(condition);

    ActionWorkflowFieldArray fields = new ActionWorkflowFieldArray();
    fields.add(triggerField);
    fields.add(conditionalField);
    form.setFields(fields);

    trigger.setForm(form);
    workflowInfo.setTrigger(trigger);

    return workflowInfo;
  }

  private ActionWorkflowInfo createTestWorkflowInfoWithNegatedConditionalField() {
    ActionWorkflowInfo workflowInfo = new ActionWorkflowInfo();
    workflowInfo.setName("Test Negated Conditional Workflow");
    workflowInfo.setCategory(ActionWorkflowCategory.ACCESS);

    // Create trigger with form
    com.linkedin.actionworkflow.ActionWorkflowTrigger trigger =
        new com.linkedin.actionworkflow.ActionWorkflowTrigger();
    trigger.setType(com.linkedin.actionworkflow.ActionWorkflowTriggerType.FORM_SUBMITTED);

    com.linkedin.actionworkflow.ActionWorkflowForm form =
        new com.linkedin.actionworkflow.ActionWorkflowForm();

    // Create trigger field
    com.linkedin.actionworkflow.ActionWorkflowField triggerField =
        new com.linkedin.actionworkflow.ActionWorkflowField();
    triggerField.setId("trigger_field");
    triggerField.setName("Trigger Field");
    triggerField.setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"));
    triggerField.setCardinality(PropertyCardinality.SINGLE);
    triggerField.setRequired(true);

    // Create conditional field (visible when trigger_field does NOT equal "HIDE")
    com.linkedin.actionworkflow.ActionWorkflowField conditionalField =
        new com.linkedin.actionworkflow.ActionWorkflowField();
    conditionalField.setId("conditional_field");
    conditionalField.setName("Conditional Field");
    conditionalField.setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"));
    conditionalField.setCardinality(PropertyCardinality.SINGLE);
    conditionalField.setRequired(true);

    // Create negated condition
    com.linkedin.actionworkflow.ActionWorkflowFieldCondition condition =
        new com.linkedin.actionworkflow.ActionWorkflowFieldCondition();
    condition.setType(
        com.linkedin.actionworkflow.ActionWorkflowFieldConditionType.SINGLE_FIELD_VALUE);

    com.linkedin.actionworkflow.ActionWorkflowSingleFieldValueCondition singleCondition =
        new com.linkedin.actionworkflow.ActionWorkflowSingleFieldValueCondition();
    singleCondition.setField("trigger_field");
    singleCondition.setValues(new StringArray(Collections.singletonList("HIDE")));
    singleCondition.setCondition(Condition.EQUAL);
    singleCondition.setNegated(true); // Negated condition

    condition.setSingleFieldValueCondition(singleCondition);
    conditionalField.setCondition(condition);

    ActionWorkflowFieldArray fields = new ActionWorkflowFieldArray();
    fields.add(triggerField);
    fields.add(conditionalField);
    form.setFields(fields);

    trigger.setForm(form);
    workflowInfo.setTrigger(trigger);

    return workflowInfo;
  }

  private ActionWorkflowInfo createTestWorkflowInfoWithContainCondition() {
    ActionWorkflowInfo workflowInfo = new ActionWorkflowInfo();
    workflowInfo.setName("Test Contain Condition Workflow");
    workflowInfo.setCategory(ActionWorkflowCategory.ACCESS);

    // Create trigger with form
    com.linkedin.actionworkflow.ActionWorkflowTrigger trigger =
        new com.linkedin.actionworkflow.ActionWorkflowTrigger();
    trigger.setType(com.linkedin.actionworkflow.ActionWorkflowTriggerType.FORM_SUBMITTED);

    com.linkedin.actionworkflow.ActionWorkflowForm form =
        new com.linkedin.actionworkflow.ActionWorkflowForm();

    // Create trigger field
    com.linkedin.actionworkflow.ActionWorkflowField triggerField =
        new com.linkedin.actionworkflow.ActionWorkflowField();
    triggerField.setId("description");
    triggerField.setName("Description");
    triggerField.setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"));
    triggerField.setCardinality(PropertyCardinality.SINGLE);
    triggerField.setRequired(true);

    // Create conditional field (visible when description contains "urgent")
    com.linkedin.actionworkflow.ActionWorkflowField conditionalField =
        new com.linkedin.actionworkflow.ActionWorkflowField();
    conditionalField.setId("priority_field");
    conditionalField.setName("Priority Field");
    conditionalField.setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"));
    conditionalField.setCardinality(PropertyCardinality.SINGLE);
    conditionalField.setRequired(true);

    // Create contain condition
    com.linkedin.actionworkflow.ActionWorkflowFieldCondition condition =
        new com.linkedin.actionworkflow.ActionWorkflowFieldCondition();
    condition.setType(
        com.linkedin.actionworkflow.ActionWorkflowFieldConditionType.SINGLE_FIELD_VALUE);

    com.linkedin.actionworkflow.ActionWorkflowSingleFieldValueCondition singleCondition =
        new com.linkedin.actionworkflow.ActionWorkflowSingleFieldValueCondition();
    singleCondition.setField("description");
    singleCondition.setValues(new StringArray(Collections.singletonList("urgent")));
    singleCondition.setCondition(Condition.CONTAIN);
    singleCondition.setNegated(false);

    condition.setSingleFieldValueCondition(singleCondition);
    conditionalField.setCondition(condition);

    ActionWorkflowFieldArray fields = new ActionWorkflowFieldArray();
    fields.add(triggerField);
    fields.add(conditionalField);
    form.setFields(fields);

    trigger.setForm(form);
    workflowInfo.setTrigger(trigger);

    return workflowInfo;
  }

  private ActionWorkflowInfo createTestWorkflowInfoWithMultipleConditionValues() {
    ActionWorkflowInfo workflowInfo = new ActionWorkflowInfo();
    workflowInfo.setName("Test Multiple Values Condition Workflow");
    workflowInfo.setCategory(ActionWorkflowCategory.ACCESS);

    // Create trigger with form
    com.linkedin.actionworkflow.ActionWorkflowTrigger trigger =
        new com.linkedin.actionworkflow.ActionWorkflowTrigger();
    trigger.setType(com.linkedin.actionworkflow.ActionWorkflowTriggerType.FORM_SUBMITTED);

    com.linkedin.actionworkflow.ActionWorkflowForm form =
        new com.linkedin.actionworkflow.ActionWorkflowForm();

    // Create trigger field
    com.linkedin.actionworkflow.ActionWorkflowField triggerField =
        new com.linkedin.actionworkflow.ActionWorkflowField();
    triggerField.setId("approval_type");
    triggerField.setName("Approval Type");
    triggerField.setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"));
    triggerField.setCardinality(PropertyCardinality.SINGLE);
    triggerField.setRequired(true);

    // Create conditional field (visible when approval_type is "ADVANCED" or "CRITICAL")
    com.linkedin.actionworkflow.ActionWorkflowField conditionalField =
        new com.linkedin.actionworkflow.ActionWorkflowField();
    conditionalField.setId("detailed_reason");
    conditionalField.setName("Detailed Reason");
    conditionalField.setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"));
    conditionalField.setCardinality(PropertyCardinality.SINGLE);
    conditionalField.setRequired(true);

    // Create condition with multiple values
    com.linkedin.actionworkflow.ActionWorkflowFieldCondition condition =
        new com.linkedin.actionworkflow.ActionWorkflowFieldCondition();
    condition.setType(
        com.linkedin.actionworkflow.ActionWorkflowFieldConditionType.SINGLE_FIELD_VALUE);

    com.linkedin.actionworkflow.ActionWorkflowSingleFieldValueCondition singleCondition =
        new com.linkedin.actionworkflow.ActionWorkflowSingleFieldValueCondition();
    singleCondition.setField("approval_type");
    singleCondition.setValues(new StringArray(Arrays.asList("ADVANCED", "CRITICAL")));
    singleCondition.setCondition(Condition.EQUAL);
    singleCondition.setNegated(false);

    condition.setSingleFieldValueCondition(singleCondition);
    conditionalField.setCondition(condition);

    ActionWorkflowFieldArray fields = new ActionWorkflowFieldArray();
    fields.add(triggerField);
    fields.add(conditionalField);
    form.setFields(fields);

    trigger.setForm(form);
    workflowInfo.setTrigger(trigger);

    return workflowInfo;
  }

  private ActionWorkflowFormRequest createWorkflowRequestWithTriggerFieldOnly() {
    ActionWorkflowFormRequest request = new ActionWorkflowFormRequest();
    request.setWorkflow(TEST_WORKFLOW_URN);
    request.setCategory(ActionWorkflowCategory.ACCESS);

    // Only provide trigger field with value that doesn't trigger the condition
    ActionWorkflowFormRequestField triggerField = new ActionWorkflowFormRequestField();
    triggerField.setId("trigger_field");
    PrimitivePropertyValueArray values = new PrimitivePropertyValueArray();
    values.add(PrimitivePropertyValue.create("HIDE")); // Doesn't trigger condition
    triggerField.setValues(values);

    com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray fields =
        new com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray();
    fields.add(triggerField);
    request.setFields(fields);

    return request;
  }

  private ActionWorkflowFormRequest createWorkflowRequestTriggeringConditionalField() {
    ActionWorkflowFormRequest request = new ActionWorkflowFormRequest();
    request.setWorkflow(TEST_WORKFLOW_URN);
    request.setCategory(ActionWorkflowCategory.ACCESS);

    // Provide trigger field with value that triggers the condition, but not the conditional field
    ActionWorkflowFormRequestField triggerField = new ActionWorkflowFormRequestField();
    triggerField.setId("trigger_field");
    PrimitivePropertyValueArray values = new PrimitivePropertyValueArray();
    values.add(PrimitivePropertyValue.create("SHOW")); // Triggers condition
    triggerField.setValues(values);

    com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray fields =
        new com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray();
    fields.add(triggerField);
    request.setFields(fields);

    return request;
  }

  private ActionWorkflowFormRequest createWorkflowRequestWithBothFields() {
    ActionWorkflowFormRequest request = new ActionWorkflowFormRequest();
    request.setWorkflow(TEST_WORKFLOW_URN);
    request.setCategory(ActionWorkflowCategory.ACCESS);

    // Provide trigger field
    ActionWorkflowFormRequestField triggerField = new ActionWorkflowFormRequestField();
    triggerField.setId("trigger_field");
    PrimitivePropertyValueArray triggerValues = new PrimitivePropertyValueArray();
    triggerValues.add(PrimitivePropertyValue.create("SHOW"));
    triggerField.setValues(triggerValues);

    // Provide conditional field
    ActionWorkflowFormRequestField conditionalField = new ActionWorkflowFormRequestField();
    conditionalField.setId("conditional_field");
    PrimitivePropertyValueArray conditionalValues = new PrimitivePropertyValueArray();
    conditionalValues.add(PrimitivePropertyValue.create("Conditional value"));
    conditionalField.setValues(conditionalValues);

    com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray fields =
        new com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray();
    fields.add(triggerField);
    fields.add(conditionalField);
    request.setFields(fields);

    return request;
  }

  private ActionWorkflowFormRequest createWorkflowRequestWithNegatedConditionValue() {
    ActionWorkflowFormRequest request = new ActionWorkflowFormRequest();
    request.setWorkflow(TEST_WORKFLOW_URN);
    request.setCategory(ActionWorkflowCategory.ACCESS);

    // Provide trigger field with value "HIDE" which should hide conditional field due to negation
    ActionWorkflowFormRequestField triggerField = new ActionWorkflowFormRequestField();
    triggerField.setId("trigger_field");
    PrimitivePropertyValueArray values = new PrimitivePropertyValueArray();
    values.add(PrimitivePropertyValue.create("HIDE"));
    triggerField.setValues(values);

    com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray fields =
        new com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray();
    fields.add(triggerField);
    request.setFields(fields);

    return request;
  }

  private ActionWorkflowFormRequest createWorkflowRequestWithContainValue() {
    ActionWorkflowFormRequest request = new ActionWorkflowFormRequest();
    request.setWorkflow(TEST_WORKFLOW_URN);
    request.setCategory(ActionWorkflowCategory.ACCESS);

    // Provide description field containing "urgent"
    ActionWorkflowFormRequestField descriptionField = new ActionWorkflowFormRequestField();
    descriptionField.setId("description");
    PrimitivePropertyValueArray values = new PrimitivePropertyValueArray();
    values.add(PrimitivePropertyValue.create("This is an urgent request"));
    descriptionField.setValues(values);

    // Provide priority field since it should be visible
    ActionWorkflowFormRequestField priorityField = new ActionWorkflowFormRequestField();
    priorityField.setId("priority_field");
    PrimitivePropertyValueArray priorityValues = new PrimitivePropertyValueArray();
    priorityValues.add(PrimitivePropertyValue.create("HIGH"));
    priorityField.setValues(priorityValues);

    com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray fields =
        new com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray();
    fields.add(descriptionField);
    fields.add(priorityField);
    request.setFields(fields);

    return request;
  }

  private ActionWorkflowFormRequest createWorkflowRequestWithOneOfMultipleValues() {
    ActionWorkflowFormRequest request = new ActionWorkflowFormRequest();
    request.setWorkflow(TEST_WORKFLOW_URN);
    request.setCategory(ActionWorkflowCategory.ACCESS);

    // Provide approval type field with "CRITICAL" (one of the allowed values)
    ActionWorkflowFormRequestField approvalField = new ActionWorkflowFormRequestField();
    approvalField.setId("approval_type");
    PrimitivePropertyValueArray values = new PrimitivePropertyValueArray();
    values.add(PrimitivePropertyValue.create("CRITICAL"));
    approvalField.setValues(values);

    // Provide detailed reason field since it should be visible
    ActionWorkflowFormRequestField reasonField = new ActionWorkflowFormRequestField();
    reasonField.setId("detailed_reason");
    PrimitivePropertyValueArray reasonValues = new PrimitivePropertyValueArray();
    reasonValues.add(PrimitivePropertyValue.create("Critical access needed"));
    reasonField.setValues(reasonValues);

    com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray fields =
        new com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray();
    fields.add(approvalField);
    fields.add(reasonField);
    request.setFields(fields);

    return request;
  }

  private ActionWorkflowFormRequest createWorkflowRequestWithHiddenFieldButEmptyValues() {
    ActionWorkflowFormRequest request = new ActionWorkflowFormRequest();
    request.setWorkflow(TEST_WORKFLOW_URN);
    request.setCategory(ActionWorkflowCategory.ACCESS);

    // Provide trigger field with value that HIDES conditional field
    ActionWorkflowFormRequestField triggerField = new ActionWorkflowFormRequestField();
    triggerField.setId("trigger_field");
    PrimitivePropertyValueArray triggerValues = new PrimitivePropertyValueArray();
    triggerValues.add(PrimitivePropertyValue.create("HIDE")); // This hides conditional_field
    triggerField.setValues(triggerValues);

    // ALSO provide the conditional field with EMPTY values (this would trigger the old bug)
    ActionWorkflowFormRequestField conditionalField = new ActionWorkflowFormRequestField();
    conditionalField.setId("conditional_field");
    conditionalField.setValues(
        new PrimitivePropertyValueArray()); // EMPTY values for required field

    com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray fields =
        new com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray();
    fields.add(triggerField);
    fields.add(conditionalField); // Include the hidden required field with empty values
    request.setFields(fields);

    return request;
  }

  private ActionWorkflowFormRequest createWorkflowRequestWithHiddenFieldButWithValues() {
    ActionWorkflowFormRequest request = new ActionWorkflowFormRequest();
    request.setWorkflow(TEST_WORKFLOW_URN);
    request.setCategory(ActionWorkflowCategory.ACCESS);

    // Provide trigger field with value that HIDES conditional field
    ActionWorkflowFormRequestField triggerField = new ActionWorkflowFormRequestField();
    triggerField.setId("trigger_field");
    PrimitivePropertyValueArray triggerValues = new PrimitivePropertyValueArray();
    triggerValues.add(PrimitivePropertyValue.create("HIDE")); // This hides conditional_field
    triggerField.setValues(triggerValues);

    // ALSO provide the conditional field with actual values (testing hidden fields can have values)
    ActionWorkflowFormRequestField conditionalField = new ActionWorkflowFormRequestField();
    conditionalField.setId("conditional_field");
    PrimitivePropertyValueArray conditionalValues = new PrimitivePropertyValueArray();
    conditionalValues.add(PrimitivePropertyValue.create("Some value")); // NON-EMPTY values
    conditionalField.setValues(conditionalValues);

    com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray fields =
        new com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray();
    fields.add(triggerField);
    fields.add(conditionalField); // Include the hidden field with values
    request.setFields(fields);

    return request;
  }

  /*--------------------------------------------------------------------------
   *                      HELPER METHODS FOR VALIDATION TESTS
   *------------------------------------------------------------------------*/

  private ActionWorkflowFormRequest createValidWorkflowRequest() {
    ActionWorkflowFormRequest request = new ActionWorkflowFormRequest();
    request.setWorkflow(TEST_WORKFLOW_URN);
    request.setCategory(ActionWorkflowCategory.ACCESS);

    // Create field that matches the test workflow
    ActionWorkflowFormRequestField field = new ActionWorkflowFormRequestField();
    field.setId("reason");
    PrimitivePropertyValueArray values = new PrimitivePropertyValueArray();
    values.add(PrimitivePropertyValue.create("Test reason"));
    field.setValues(values);

    com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray fields =
        new com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray();
    fields.add(field);
    request.setFields(fields);

    return request;
  }

  private ActionWorkflowFormRequest createEmptyFieldsWorkflowRequest() {
    ActionWorkflowFormRequest request = new ActionWorkflowFormRequest();
    request.setWorkflow(TEST_WORKFLOW_URN);
    request.setCategory(ActionWorkflowCategory.ACCESS);
    request.setFields(
        new com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray()); // Empty
    return request;
  }

  private ActionWorkflowFormRequest createWorkflowRequestWithInvalidField() {
    ActionWorkflowFormRequest request = new ActionWorkflowFormRequest();
    request.setWorkflow(TEST_WORKFLOW_URN);
    request.setCategory(ActionWorkflowCategory.ACCESS);

    // Create field with invalid ID
    ActionWorkflowFormRequestField field = new ActionWorkflowFormRequestField();
    field.setId("invalid_field_id");
    PrimitivePropertyValueArray values = new PrimitivePropertyValueArray();
    values.add(PrimitivePropertyValue.create("Test value"));
    field.setValues(values);

    com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray fields =
        new com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray();
    fields.add(field);
    request.setFields(fields);

    return request;
  }

  private ActionWorkflowFormRequest createWorkflowRequestMissingRequiredField() {
    ActionWorkflowFormRequest request = new ActionWorkflowFormRequest();
    request.setWorkflow(TEST_WORKFLOW_URN);
    request.setCategory(ActionWorkflowCategory.ACCESS);

    // Only provide the optional field, not the required one
    ActionWorkflowFormRequestField field = new ActionWorkflowFormRequestField();
    field.setId("optional_field");
    PrimitivePropertyValueArray values = new PrimitivePropertyValueArray();
    values.add(PrimitivePropertyValue.create("Optional value"));
    field.setValues(values);

    com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray fields =
        new com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray();
    fields.add(field);
    request.setFields(fields);

    return request;
  }

  private ActionWorkflowFormRequest createWorkflowRequestWithMultipleValues() {
    ActionWorkflowFormRequest request = new ActionWorkflowFormRequest();
    request.setWorkflow(TEST_WORKFLOW_URN);
    request.setCategory(ActionWorkflowCategory.ACCESS);

    // Create field with multiple values for single cardinality field
    ActionWorkflowFormRequestField field = new ActionWorkflowFormRequestField();
    field.setId("single_field");
    PrimitivePropertyValueArray values = new PrimitivePropertyValueArray();
    values.add(PrimitivePropertyValue.create("Value 1"));
    values.add(PrimitivePropertyValue.create("Value 2")); // Multiple values
    field.setValues(values);

    com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray fields =
        new com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray();
    fields.add(field);
    request.setFields(fields);

    return request;
  }

  private ActionWorkflowFormRequest createWorkflowRequestWithEmptyFieldValues() {
    ActionWorkflowFormRequest request = new ActionWorkflowFormRequest();
    request.setWorkflow(TEST_WORKFLOW_URN);
    request.setCategory(ActionWorkflowCategory.ACCESS);

    // Create field with empty values
    ActionWorkflowFormRequestField field = new ActionWorkflowFormRequestField();
    field.setId("reason");
    field.setValues(new PrimitivePropertyValueArray()); // Empty values

    com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray fields =
        new com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray();
    fields.add(field);
    request.setFields(fields);

    return request;
  }

  private ActionWorkflowFormRequest createWorkflowRequestWithEmptyFieldValuesForRequiredField() {
    ActionWorkflowFormRequest request = new ActionWorkflowFormRequest();
    request.setWorkflow(TEST_WORKFLOW_URN);
    request.setCategory(ActionWorkflowCategory.ACCESS);

    // Create field with empty values for a required field
    ActionWorkflowFormRequestField field = new ActionWorkflowFormRequestField();
    field.setId("required_field");
    field.setValues(new PrimitivePropertyValueArray()); // Empty values

    com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray fields =
        new com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray();
    fields.add(field);
    request.setFields(fields);

    return request;
  }

  private ActionWorkflowFormRequest createWorkflowRequestWithInvalidStringValue() {
    ActionWorkflowFormRequest request = new ActionWorkflowFormRequest();
    request.setWorkflow(TEST_WORKFLOW_URN);
    request.setCategory(ActionWorkflowCategory.ACCESS);

    ActionWorkflowFormRequestField field = new ActionWorkflowFormRequestField();
    field.setId("classification");
    PrimitivePropertyValueArray values = new PrimitivePropertyValueArray();
    values.add(PrimitivePropertyValue.create("INVALID_CLASSIFICATION")); // Not in allowed values
    field.setValues(values);

    com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray fields =
        new com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray();
    fields.add(field);
    request.setFields(fields);

    return request;
  }

  private ActionWorkflowFormRequest createWorkflowRequestWithInvalidNumberValue() {
    ActionWorkflowFormRequest request = new ActionWorkflowFormRequest();
    request.setWorkflow(TEST_WORKFLOW_URN);
    request.setCategory(ActionWorkflowCategory.ACCESS);

    ActionWorkflowFormRequestField field = new ActionWorkflowFormRequestField();
    field.setId("priority");
    PrimitivePropertyValueArray values = new PrimitivePropertyValueArray();
    values.add(PrimitivePropertyValue.create(999.0)); // Not in allowed values (1-5)
    field.setValues(values);

    com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray fields =
        new com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray();
    fields.add(field);
    request.setFields(fields);

    return request;
  }

  private ActionWorkflowFormRequest createWorkflowRequestWithValidAllowedValue() {
    ActionWorkflowFormRequest request = new ActionWorkflowFormRequest();
    request.setWorkflow(TEST_WORKFLOW_URN);
    request.setCategory(ActionWorkflowCategory.ACCESS);

    ActionWorkflowFormRequestField field = new ActionWorkflowFormRequestField();
    field.setId("classification");
    PrimitivePropertyValueArray values = new PrimitivePropertyValueArray();
    values.add(PrimitivePropertyValue.create("CONFIDENTIAL")); // Valid allowed value
    field.setValues(values);

    com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray fields =
        new com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray();
    fields.add(field);
    request.setFields(fields);

    return request;
  }

  private ActionWorkflowFormRequest createWorkflowRequestWithMultipleFieldsValid() {
    ActionWorkflowFormRequest request = new ActionWorkflowFormRequest();
    request.setWorkflow(TEST_WORKFLOW_URN);
    request.setCategory(ActionWorkflowCategory.ACCESS);

    com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray fields =
        new com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray();

    // Required field
    ActionWorkflowFormRequestField requiredField = new ActionWorkflowFormRequestField();
    requiredField.setId("required_field");
    PrimitivePropertyValueArray requiredValues = new PrimitivePropertyValueArray();
    requiredValues.add(PrimitivePropertyValue.create("Required value"));
    requiredField.setValues(requiredValues);
    fields.add(requiredField);

    // Optional field
    ActionWorkflowFormRequestField optionalField = new ActionWorkflowFormRequestField();
    optionalField.setId("optional_field");
    PrimitivePropertyValueArray optionalValues = new PrimitivePropertyValueArray();
    optionalValues.add(PrimitivePropertyValue.create("Optional value"));
    optionalField.setValues(optionalValues);
    fields.add(optionalField);

    request.setFields(fields);
    return request;
  }

  /*--------------------------------------------------------------------------
   *                      HELPER METHODS FOR WORKFLOW INFO CREATION
   *------------------------------------------------------------------------*/

  private ActionWorkflowInfo createTestWorkflowInfoWithoutFields() {
    ActionWorkflowInfo workflowInfo = new ActionWorkflowInfo();
    workflowInfo.setName("Test Workflow Without Fields");
    workflowInfo.setCategory(ActionWorkflowCategory.ACCESS);
    workflowInfo.setDescription("Test workflow with no fields");

    // Create trigger with form but no fields
    com.linkedin.actionworkflow.ActionWorkflowTrigger trigger =
        new com.linkedin.actionworkflow.ActionWorkflowTrigger();
    trigger.setType(com.linkedin.actionworkflow.ActionWorkflowTriggerType.FORM_SUBMITTED);

    com.linkedin.actionworkflow.ActionWorkflowForm form =
        new com.linkedin.actionworkflow.ActionWorkflowForm();
    form.setFields(new ActionWorkflowFieldArray()); // Empty fields
    form.setEntrypoints(new ActionWorkflowEntrypointArray());

    trigger.setForm(form);
    workflowInfo.setTrigger(trigger);
    workflowInfo.setSteps(new ActionWorkflowStepArray());
    return workflowInfo;
  }

  private ActionWorkflowInfo createTestWorkflowInfoWithMultipleFields() {
    ActionWorkflowInfo workflowInfo = new ActionWorkflowInfo();
    workflowInfo.setName("Test Workflow With Multiple Fields");
    workflowInfo.setCategory(ActionWorkflowCategory.ACCESS);
    workflowInfo.setDescription("Test workflow with multiple fields");

    // Create trigger with form
    com.linkedin.actionworkflow.ActionWorkflowTrigger trigger =
        new com.linkedin.actionworkflow.ActionWorkflowTrigger();
    trigger.setType(com.linkedin.actionworkflow.ActionWorkflowTriggerType.FORM_SUBMITTED);

    com.linkedin.actionworkflow.ActionWorkflowForm form =
        new com.linkedin.actionworkflow.ActionWorkflowForm();

    ActionWorkflowFieldArray fields = new ActionWorkflowFieldArray();

    // Required field
    com.linkedin.actionworkflow.ActionWorkflowField requiredField =
        new com.linkedin.actionworkflow.ActionWorkflowField();
    requiredField.setId("required_field");
    requiredField.setName("Required Field");
    requiredField.setDescription("A required field");
    requiredField.setValueType(UrnUtils.getUrn("urn:li:structured:propertyType:string"));
    requiredField.setCardinality(com.linkedin.structured.PropertyCardinality.SINGLE);
    requiredField.setRequired(true);
    fields.add(requiredField);

    // Optional field
    com.linkedin.actionworkflow.ActionWorkflowField optionalField =
        new com.linkedin.actionworkflow.ActionWorkflowField();
    optionalField.setId("optional_field");
    optionalField.setName("Optional Field");
    optionalField.setDescription("An optional field");
    optionalField.setValueType(UrnUtils.getUrn("urn:li:structured:propertyType:string"));
    optionalField.setCardinality(com.linkedin.structured.PropertyCardinality.SINGLE);
    optionalField.setRequired(false);
    fields.add(optionalField);

    form.setFields(fields);
    form.setEntrypoints(new ActionWorkflowEntrypointArray());
    trigger.setForm(form);
    workflowInfo.setTrigger(trigger);
    workflowInfo.setSteps(new ActionWorkflowStepArray());
    return workflowInfo;
  }

  private ActionWorkflowInfo createTestWorkflowInfoWithSingleCardinalityField() {
    ActionWorkflowInfo workflowInfo = new ActionWorkflowInfo();
    workflowInfo.setName("Test Workflow With Single Cardinality");
    workflowInfo.setCategory(ActionWorkflowCategory.ACCESS);
    workflowInfo.setDescription("Test workflow with single cardinality field");

    // Create trigger with form
    com.linkedin.actionworkflow.ActionWorkflowTrigger trigger =
        new com.linkedin.actionworkflow.ActionWorkflowTrigger();
    trigger.setType(com.linkedin.actionworkflow.ActionWorkflowTriggerType.FORM_SUBMITTED);

    com.linkedin.actionworkflow.ActionWorkflowForm form =
        new com.linkedin.actionworkflow.ActionWorkflowForm();

    ActionWorkflowFieldArray fields = new ActionWorkflowFieldArray();

    com.linkedin.actionworkflow.ActionWorkflowField singleField =
        new com.linkedin.actionworkflow.ActionWorkflowField();
    singleField.setId("single_field");
    singleField.setName("Single Field");
    singleField.setDescription("A field with single cardinality");
    singleField.setValueType(UrnUtils.getUrn("urn:li:structured:propertyType:string"));
    singleField.setCardinality(com.linkedin.structured.PropertyCardinality.SINGLE);
    singleField.setRequired(true);
    fields.add(singleField);

    form.setFields(fields);
    form.setEntrypoints(new ActionWorkflowEntrypointArray());
    trigger.setForm(form);
    workflowInfo.setTrigger(trigger);
    workflowInfo.setSteps(new ActionWorkflowStepArray());
    return workflowInfo;
  }

  private ActionWorkflowInfo createTestWorkflowInfoWithAllowedValues() {
    ActionWorkflowInfo workflowInfo = new ActionWorkflowInfo();
    workflowInfo.setName("Test Workflow With Allowed Values");
    workflowInfo.setCategory(ActionWorkflowCategory.CUSTOM);
    workflowInfo.setDescription("Test workflow with allowed values");

    // Create trigger with form
    com.linkedin.actionworkflow.ActionWorkflowTrigger trigger =
        new com.linkedin.actionworkflow.ActionWorkflowTrigger();
    trigger.setType(com.linkedin.actionworkflow.ActionWorkflowTriggerType.FORM_SUBMITTED);

    com.linkedin.actionworkflow.ActionWorkflowForm form =
        new com.linkedin.actionworkflow.ActionWorkflowForm();

    ActionWorkflowFieldArray fields = new ActionWorkflowFieldArray();

    com.linkedin.actionworkflow.ActionWorkflowField classificationField =
        new com.linkedin.actionworkflow.ActionWorkflowField();
    classificationField.setId("classification");
    classificationField.setName("Classification Level");
    classificationField.setDescription("Classification with allowed values");
    classificationField.setValueType(UrnUtils.getUrn("urn:li:structured:propertyType:string"));
    classificationField.setCardinality(com.linkedin.structured.PropertyCardinality.SINGLE);
    classificationField.setRequired(true);

    // Set allowed values
    com.linkedin.structured.PropertyValueArray allowedValues =
        new com.linkedin.structured.PropertyValueArray();
    allowedValues.add(
        new com.linkedin.structured.PropertyValue()
            .setValue(PrimitivePropertyValue.create("PUBLIC")));
    allowedValues.add(
        new com.linkedin.structured.PropertyValue()
            .setValue(PrimitivePropertyValue.create("INTERNAL")));
    allowedValues.add(
        new com.linkedin.structured.PropertyValue()
            .setValue(PrimitivePropertyValue.create("CONFIDENTIAL")));
    classificationField.setAllowedValues(allowedValues);

    fields.add(classificationField);
    form.setFields(fields);
    form.setEntrypoints(new ActionWorkflowEntrypointArray());
    trigger.setForm(form);
    workflowInfo.setTrigger(trigger);
    workflowInfo.setSteps(new ActionWorkflowStepArray());
    return workflowInfo;
  }

  private ActionWorkflowInfo createTestWorkflowInfoWithAllowedNumberValues() {
    ActionWorkflowInfo workflowInfo = new ActionWorkflowInfo();
    workflowInfo.setName("Test Workflow With Allowed Number Values");
    workflowInfo.setCategory(ActionWorkflowCategory.CUSTOM);
    workflowInfo.setDescription("Test workflow with allowed number values");

    // Create trigger with form
    com.linkedin.actionworkflow.ActionWorkflowTrigger trigger =
        new com.linkedin.actionworkflow.ActionWorkflowTrigger();
    trigger.setType(com.linkedin.actionworkflow.ActionWorkflowTriggerType.FORM_SUBMITTED);

    com.linkedin.actionworkflow.ActionWorkflowForm form =
        new com.linkedin.actionworkflow.ActionWorkflowForm();

    ActionWorkflowFieldArray fields = new ActionWorkflowFieldArray();

    com.linkedin.actionworkflow.ActionWorkflowField priorityField =
        new com.linkedin.actionworkflow.ActionWorkflowField();
    priorityField.setId("priority");
    priorityField.setName("Priority Level");
    priorityField.setDescription("Priority with allowed number values");
    priorityField.setValueType(UrnUtils.getUrn("urn:li:structured:propertyType:number"));
    priorityField.setCardinality(com.linkedin.structured.PropertyCardinality.SINGLE);
    priorityField.setRequired(true);

    // Set allowed number values (1-5)
    com.linkedin.structured.PropertyValueArray allowedValues =
        new com.linkedin.structured.PropertyValueArray();
    for (int i = 1; i <= 5; i++) {
      allowedValues.add(
          new com.linkedin.structured.PropertyValue()
              .setValue(PrimitivePropertyValue.create((double) i)));
    }
    priorityField.setAllowedValues(allowedValues);

    fields.add(priorityField);
    form.setFields(fields);
    form.setEntrypoints(new ActionWorkflowEntrypointArray());
    trigger.setForm(form);
    workflowInfo.setTrigger(trigger);
    workflowInfo.setSteps(new ActionWorkflowStepArray());
    return workflowInfo;
  }

  /*--------------------------------------------------------------------------
   *                      EXISTING HELPER METHODS
   *------------------------------------------------------------------------*/

  private ActionWorkflowInfo createTestWorkflowInfo() {
    ActionWorkflowInfo workflowInfo = new ActionWorkflowInfo();
    workflowInfo.setName("Test Workflow");
    workflowInfo.setCategory(ActionWorkflowCategory.ACCESS);
    workflowInfo.setDescription("Test workflow description");
    workflowInfo.setCustomCategory("custom-category");

    // Set audit stamps
    AuditStamp auditStamp =
        new AuditStamp().setTime(System.currentTimeMillis()).setActor(TEST_ACTOR_URN);
    workflowInfo.setCreated(auditStamp);
    workflowInfo.setLastModified(auditStamp);

    // Create trigger with form
    com.linkedin.actionworkflow.ActionWorkflowTrigger trigger =
        new com.linkedin.actionworkflow.ActionWorkflowTrigger();
    trigger.setType(com.linkedin.actionworkflow.ActionWorkflowTriggerType.FORM_SUBMITTED);

    com.linkedin.actionworkflow.ActionWorkflowForm form =
        new com.linkedin.actionworkflow.ActionWorkflowForm();

    // Set entity types in trigger form
    UrnArray entityTypes = new UrnArray();
    entityTypes.add(UrnUtils.getUrn("urn:li:entityType:dataset"));
    form.setEntityTypes(entityTypes);

    // Create fields array with field1 in trigger form
    ActionWorkflowFieldArray fields = new ActionWorkflowFieldArray();
    com.linkedin.actionworkflow.ActionWorkflowField field1 =
        new com.linkedin.actionworkflow.ActionWorkflowField();
    field1.setId("field1");
    field1.setName("Field 1");
    field1.setDescription("Test field 1");
    field1.setValueType(UrnUtils.getUrn("urn:li:structured:propertyType:string"));
    field1.setCardinality(com.linkedin.structured.PropertyCardinality.SINGLE);
    field1.setRequired(false);
    fields.add(field1);
    form.setFields(fields);

    // Set entrypoints in trigger form
    ActionWorkflowEntrypoint entrypoint = new ActionWorkflowEntrypoint();
    entrypoint.setType(ActionWorkflowEntrypointType.ENTITY_PROFILE);
    entrypoint.setLabel("Request Access");
    form.setEntrypoints(new ActionWorkflowEntrypointArray(entrypoint));

    trigger.setForm(form);
    workflowInfo.setTrigger(trigger);

    // Set steps
    ActionWorkflowStep step = new ActionWorkflowStep();
    step.setId("step1");
    step.setType(ActionWorkflowStepType.APPROVAL);
    step.setDescription("First step");

    ActionWorkflowStepActors actors = new ActionWorkflowStepActors();
    UrnArray users = new UrnArray();
    users.add(TEST_USER_URN);
    actors.setUsers(users);
    step.setActors(actors);

    workflowInfo.setSteps(new ActionWorkflowStepArray(step));

    return workflowInfo;
  }

  private ActionWorkflowInfo createTestWorkflowInfoWithDynamicAssignment() {
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfo();

    ActionWorkflowStep step = workflowInfo.getSteps().get(0);
    ActionWorkflowStepActors actors = step.getActors();

    ActionWorkflowStepDynamicAssignment dynamicAssignment =
        new ActionWorkflowStepDynamicAssignment();
    dynamicAssignment.setType(ActionWorkflowStepDynamicAssignmentType.ENTITY_OWNERS);
    actors.setDynamicAssignment(dynamicAssignment);

    return workflowInfo;
  }

  private ActionWorkflowInfo createTestWorkflowInfoWithDomainDynamicAssignment() {
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfo();

    ActionWorkflowStep step = workflowInfo.getSteps().get(0);
    ActionWorkflowStepActors actors = step.getActors();

    ActionWorkflowStepDynamicAssignment dynamicAssignment =
        new ActionWorkflowStepDynamicAssignment();
    dynamicAssignment.setType(ActionWorkflowStepDynamicAssignmentType.ENTITY_DOMAIN_OWNERS);
    actors.setDynamicAssignment(dynamicAssignment);

    return workflowInfo;
  }

  private ActionWorkflowInfo createTestWorkflowInfoWithDataProductDynamicAssignment() {
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfo();

    ActionWorkflowStep step = workflowInfo.getSteps().get(0);
    ActionWorkflowStepActors actors = step.getActors();

    ActionWorkflowStepDynamicAssignment dynamicAssignment =
        new ActionWorkflowStepDynamicAssignment();
    dynamicAssignment.setType(ActionWorkflowStepDynamicAssignmentType.ENTITY_DATA_PRODUCT_OWNERS);
    actors.setDynamicAssignment(dynamicAssignment);

    return workflowInfo;
  }

  private List<ActionWorkflowFormRequestField> createTestRequestFields() {
    ActionWorkflowFormRequestField field = new ActionWorkflowFormRequestField();
    field.setId("field1");
    PrimitivePropertyValueArray values = new PrimitivePropertyValueArray();
    values.add(PrimitivePropertyValue.create("test-value"));
    field.setValues(values);
    return Collections.singletonList(field);
  }

  private com.linkedin.common.Owner createTestOwner(Urn ownerUrn) {
    com.linkedin.common.Owner owner = new com.linkedin.common.Owner();
    owner.setOwner(ownerUrn);
    owner.setType(com.linkedin.common.OwnershipType.DATAOWNER);
    return owner;
  }

  private EntityResponse createMockEntityResponse(ActionWorkflowInfo workflowInfo) {
    EntityResponse response = new EntityResponse();
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setName(ACTION_WORKFLOW_INFO_ASPECT_NAME);
    envelopedAspect.setValue(new com.linkedin.entity.Aspect(workflowInfo.data()));

    aspects.put(ACTION_WORKFLOW_INFO_ASPECT_NAME, envelopedAspect);
    response.setAspects(aspects);
    response.setUrn(TEST_WORKFLOW_URN);
    response.setEntityName(ACTION_WORKFLOW_ENTITY_NAME);

    return response;
  }

  private SearchResult createMockSearchResult() {
    SearchResult result = new SearchResult();
    result.setNumEntities(1);
    result.setFrom(0);
    result.setPageSize(10);

    SearchEntity entity = new SearchEntity();
    entity.setEntity(TEST_WORKFLOW_URN);
    SearchEntityArray entities = new SearchEntityArray();
    entities.add(entity);
    result.setEntities(entities);

    return result;
  }

  private SearchResult createEmptySearchResult() {
    SearchResult result = new SearchResult();
    result.setNumEntities(0);
    result.setFrom(0);
    result.setPageSize(10);
    result.setEntities(new SearchEntityArray());
    return result;
  }

  private ActionRequestInfo createTestActionRequestInfo() {
    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType("WORKFLOW_FORM_REQUEST");
    actionRequestInfo.setDueDate(System.currentTimeMillis() + 86400000L);
    actionRequestInfo.setCreatedBy(TEST_ACTOR_URN);
    actionRequestInfo.setCreated(System.currentTimeMillis());
    actionRequestInfo.setDescription("Test action request");

    // Set assigned users
    UrnArray assignedUsers = new UrnArray();
    assignedUsers.add(TEST_USER_URN);
    actionRequestInfo.setAssignedUsers(assignedUsers);

    // Set assigned groups
    UrnArray assignedGroups = new UrnArray();
    assignedGroups.add(TEST_GROUP_URN);
    actionRequestInfo.setAssignedGroups(assignedGroups);

    // Set assigned roles
    UrnArray assignedRoles = new UrnArray();
    assignedRoles.add(TEST_ROLE_URN);
    actionRequestInfo.setAssignedRoles(assignedRoles);

    // Create and set workflow request
    ActionWorkflowFormRequest workflowRequest = new ActionWorkflowFormRequest();
    workflowRequest.setWorkflow(TEST_WORKFLOW_URN);
    workflowRequest.setCategory(ActionWorkflowCategory.ACCESS);
    workflowRequest.setFields(new ActionWorkflowFormRequestFieldArray());

    // Create step state
    com.linkedin.actionworkflow.ActionWorkflowRequestStepState stepState =
        new com.linkedin.actionworkflow.ActionWorkflowRequestStepState();
    stepState.setStepId("step1");
    workflowRequest.setStepState(stepState);

    com.linkedin.actionrequest.ActionRequestParams params =
        new com.linkedin.actionrequest.ActionRequestParams();
    params.setWorkflowFormRequest(workflowRequest);
    actionRequestInfo.setParams(params);

    return actionRequestInfo;
  }

  private ActionRequestInfo createTestActionRequestInfoWithFinalStep() {
    ActionRequestInfo actionRequestInfo = createTestActionRequestInfo();

    // Set step state to final step
    com.linkedin.actionworkflow.ActionWorkflowRequestStepState stepState =
        new com.linkedin.actionworkflow.ActionWorkflowRequestStepState();
    stepState.setStepId("step1"); // Assuming this is the final step
    actionRequestInfo.getParams().getWorkflowFormRequest().setStepState(stepState);

    return actionRequestInfo;
  }

  private ActionRequestInfo createTestActionRequestInfoWithoutWorkflowRequest() {
    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType("NORMAL_REQUEST");
    actionRequestInfo.setDueDate(System.currentTimeMillis() + 86400000L);
    actionRequestInfo.setCreatedBy(TEST_ACTOR_URN);
    actionRequestInfo.setCreated(System.currentTimeMillis());
    actionRequestInfo.setDescription("Test action request without workflow");

    // Set assigned users
    UrnArray assignedUsers = new UrnArray();
    assignedUsers.add(TEST_USER_URN);
    actionRequestInfo.setAssignedUsers(assignedUsers);

    // No workflow request in params
    return actionRequestInfo;
  }

  private ActionRequestInfo createTestActionRequestInfoWithDirectUserAssignment() {
    ActionRequestInfo actionRequestInfo = createTestActionRequestInfo();

    // Set assigned users directly
    UrnArray assignedUsers = new UrnArray();
    assignedUsers.add(TEST_USER_URN);
    actionRequestInfo.setAssignedUsers(assignedUsers);

    return actionRequestInfo;
  }

  private EntityResponse createMockActionRequestEntityResponse(
      ActionRequestInfo actionRequestInfo) {
    EntityResponse response = new EntityResponse();
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setName(ACTION_REQUEST_INFO_ASPECT_NAME);
    envelopedAspect.setValue(new com.linkedin.entity.Aspect(actionRequestInfo.data()));

    aspects.put(ACTION_REQUEST_INFO_ASPECT_NAME, envelopedAspect);
    response.setAspects(aspects);
    response.setUrn(UrnUtils.getUrn("urn:li:actionRequest:test-request"));
    response.setEntityName(ACTION_REQUEST_ENTITY_NAME);

    return response;
  }

  /*--------------------------------------------------------------------------
   *             OWNERSHIP TYPE FILTERING TESTS
   *------------------------------------------------------------------------*/

  @Test
  public void testCreateActionWorkflowRequestWithOwnershipTypeFiltering() throws Exception {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfoWithOwnershipTypeFiltering();
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(createMockEntityResponse(workflowInfo));

    List<ActionWorkflowFormRequestField> fields = createTestRequestFields();

    // Mock owner resolution with mixed ownership types
    when(mockOwnerService.getEntityOwners(eq(mockOpContext), eq(TEST_ENTITY_URN)))
        .thenReturn(createMixedOwnershipTypeOwners());

    ArgumentCaptor<Collection<com.linkedin.mxe.MetadataChangeProposal>> proposalsCaptor =
        ArgumentCaptor.forClass(Collection.class);

    // WHEN
    Urn result =
        actionWorkflowService.createActionWorkflowFormRequest(
            TEST_WORKFLOW_URN, TEST_ENTITY_URN, "Test description", fields, null, mockOpContext);

    // THEN
    assertNotNull(result);
    verify(mockOwnerService).getEntityOwners(eq(mockOpContext), eq(TEST_ENTITY_URN));

    // Verify that batchIngestProposals is called with filtered owners
    verify(mockEntityClient, times(1))
        .batchIngestProposals(eq(mockOpContext), proposalsCaptor.capture(), eq(false));

    Collection<com.linkedin.mxe.MetadataChangeProposal> proposals = proposalsCaptor.getValue();
    assertEquals(proposals.size(), 2, "Should batch ingest exactly 2 proposals");

    // Find the ActionRequestInfo proposal and verify ownership filtering
    boolean foundInfoProposal = false;
    for (com.linkedin.mxe.MetadataChangeProposal proposal : proposals) {
      if (ACTION_REQUEST_INFO_ASPECT_NAME.equals(proposal.getAspectName())) {
        foundInfoProposal = true;

        // Deserialize the ActionRequestInfo to check assigned users
        ActionRequestInfo actionRequestInfo =
            GenericRecordUtils.deserializeAspect(
                proposal.getAspect().getValue(),
                proposal.getAspect().getContentType(),
                ActionRequestInfo.class);

        // Should only contain users with TECHNICAL_OWNER type (not BUSINESS_OWNER or no type)
        List<Urn> assignedUsers = actionRequestInfo.getAssignedUsers();
        assertEquals(assignedUsers.size(), 1, "Should only have technical owners assigned");
        assertTrue(assignedUsers.contains(TEST_USER_URN), "Should contain the technical owner");
        break;
      }
    }

    assertTrue(foundInfoProposal, "ActionRequestInfo aspect should be batched");
  }

  @Test
  public void testCreateActionWorkflowRequestWithNoOwnershipTypeFiltering() throws Exception {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfoWithoutOwnershipTypeFiltering();
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(createMockEntityResponse(workflowInfo));

    List<ActionWorkflowFormRequestField> fields = createTestRequestFields();

    // Mock owner resolution with mixed ownership types
    when(mockOwnerService.getEntityOwners(eq(mockOpContext), eq(TEST_ENTITY_URN)))
        .thenReturn(createMixedOwnershipTypeOwners());

    ArgumentCaptor<Collection<com.linkedin.mxe.MetadataChangeProposal>> proposalsCaptor =
        ArgumentCaptor.forClass(Collection.class);

    // WHEN
    Urn result =
        actionWorkflowService.createActionWorkflowFormRequest(
            TEST_WORKFLOW_URN, TEST_ENTITY_URN, "Test description", fields, null, mockOpContext);

    // THEN
    assertNotNull(result);
    verify(mockOwnerService).getEntityOwners(eq(mockOpContext), eq(TEST_ENTITY_URN));

    // Verify that batchIngestProposals is called with all owners (no filtering)
    verify(mockEntityClient, times(1))
        .batchIngestProposals(eq(mockOpContext), proposalsCaptor.capture(), eq(false));

    Collection<com.linkedin.mxe.MetadataChangeProposal> proposals = proposalsCaptor.getValue();
    assertEquals(proposals.size(), 2, "Should batch ingest exactly 2 proposals");

    // Find the ActionRequestInfo proposal and verify no ownership filtering
    boolean foundInfoProposal = false;
    for (com.linkedin.mxe.MetadataChangeProposal proposal : proposals) {
      if (ACTION_REQUEST_INFO_ASPECT_NAME.equals(proposal.getAspectName())) {
        foundInfoProposal = true;

        // Deserialize the ActionRequestInfo to check assigned users
        ActionRequestInfo actionRequestInfo =
            GenericRecordUtils.deserializeAspect(
                proposal.getAspect().getValue(),
                proposal.getAspect().getContentType(),
                ActionRequestInfo.class);

        // Should contain all users (no filtering applied)
        List<Urn> assignedUsers = actionRequestInfo.getAssignedUsers();
        assertEquals(assignedUsers.size(), 3, "Should have all owners assigned when no filtering");
        break;
      }
    }

    assertTrue(foundInfoProposal, "ActionRequestInfo aspect should be batched");
  }

  @Test
  public void testCreateActionWorkflowRequestWithDomainOwnershipTypeFiltering() throws Exception {
    // GIVEN
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfoWithDomainOwnershipTypeFiltering();
    when(mockEntityClient.getV2(
            eq(mockOpContext), eq(ACTION_WORKFLOW_ENTITY_NAME), eq(TEST_WORKFLOW_URN), any()))
        .thenReturn(createMockEntityResponse(workflowInfo));

    List<ActionWorkflowFormRequestField> fields = createTestRequestFields();

    // Mock domain resolution
    when(mockDomainService.getEntityDomains(eq(mockOpContext), eq(TEST_ENTITY_URN)))
        .thenReturn(Arrays.asList(TEST_DOMAIN_URN));

    // Mock domain owner resolution with mixed ownership types
    when(mockOwnerService.getEntityOwners(eq(mockOpContext), eq(TEST_DOMAIN_URN)))
        .thenReturn(createMixedOwnershipTypeOwners());

    ArgumentCaptor<Collection<com.linkedin.mxe.MetadataChangeProposal>> proposalsCaptor =
        ArgumentCaptor.forClass(Collection.class);

    // WHEN
    Urn result =
        actionWorkflowService.createActionWorkflowFormRequest(
            TEST_WORKFLOW_URN, TEST_ENTITY_URN, "Test description", fields, null, mockOpContext);

    // THEN
    assertNotNull(result);
    verify(mockDomainService).getEntityDomains(eq(mockOpContext), eq(TEST_ENTITY_URN));
    verify(mockOwnerService).getEntityOwners(eq(mockOpContext), eq(TEST_DOMAIN_URN));

    // Verify that batchIngestProposals is called with filtered domain owners
    verify(mockEntityClient, times(1))
        .batchIngestProposals(eq(mockOpContext), proposalsCaptor.capture(), eq(false));

    Collection<com.linkedin.mxe.MetadataChangeProposal> proposals = proposalsCaptor.getValue();
    assertEquals(proposals.size(), 2, "Should batch ingest exactly 2 proposals");

    // Find the ActionRequestInfo proposal and verify ownership filtering on domain owners
    boolean foundInfoProposal = false;
    for (com.linkedin.mxe.MetadataChangeProposal proposal : proposals) {
      if (ACTION_REQUEST_INFO_ASPECT_NAME.equals(proposal.getAspectName())) {
        foundInfoProposal = true;

        // Deserialize the ActionRequestInfo to check assigned users
        ActionRequestInfo actionRequestInfo =
            GenericRecordUtils.deserializeAspect(
                proposal.getAspect().getValue(),
                proposal.getAspect().getContentType(),
                ActionRequestInfo.class);

        // Should only contain users with BUSINESS_OWNER type from domain
        List<Urn> assignedUsers = actionRequestInfo.getAssignedUsers();
        assertEquals(
            assignedUsers.size(), 1, "Should only have business owners from domain assigned");
        break;
      }
    }

    assertTrue(foundInfoProposal, "ActionRequestInfo aspect should be batched");
  }

  /*--------------------------------------------------------------------------
   *             HELPER METHODS FOR OWNERSHIP TYPE FILTERING TESTS
   *------------------------------------------------------------------------*/

  private ActionWorkflowInfo createTestWorkflowInfoWithOwnershipTypeFiltering() {
    ActionWorkflowInfo workflowInfo = new ActionWorkflowInfo();
    workflowInfo.setName("Test Workflow");
    workflowInfo.setCategory(ActionWorkflowCategory.ACCESS);
    workflowInfo.setDescription("Test workflow description");
    workflowInfo.setCustomCategory("custom-category");

    // Set audit stamps
    AuditStamp auditStamp =
        new AuditStamp().setTime(System.currentTimeMillis()).setActor(TEST_ACTOR_URN);
    workflowInfo.setCreated(auditStamp);
    workflowInfo.setLastModified(auditStamp);

    // Create trigger with form
    com.linkedin.actionworkflow.ActionWorkflowTrigger trigger =
        new com.linkedin.actionworkflow.ActionWorkflowTrigger();
    trigger.setType(com.linkedin.actionworkflow.ActionWorkflowTriggerType.FORM_SUBMITTED);

    com.linkedin.actionworkflow.ActionWorkflowForm form =
        new com.linkedin.actionworkflow.ActionWorkflowForm();

    // Set entity types in trigger form
    UrnArray entityTypes = new UrnArray();
    entityTypes.add(UrnUtils.getUrn("urn:li:entityType:dataset"));
    form.setEntityTypes(entityTypes);

    // Create fields array with field1 in trigger form
    ActionWorkflowFieldArray fields = new ActionWorkflowFieldArray();
    com.linkedin.actionworkflow.ActionWorkflowField field1 =
        new com.linkedin.actionworkflow.ActionWorkflowField();
    field1.setId("field1");
    field1.setName("Field 1");
    field1.setDescription("Test field 1");
    field1.setValueType(UrnUtils.getUrn("urn:li:structured:propertyType:string"));
    field1.setCardinality(com.linkedin.structured.PropertyCardinality.SINGLE);
    field1.setRequired(false);
    fields.add(field1);
    form.setFields(fields);

    // Set entrypoints in trigger form
    ActionWorkflowEntrypoint entrypoint = new ActionWorkflowEntrypoint();
    entrypoint.setType(ActionWorkflowEntrypointType.ENTITY_PROFILE);
    entrypoint.setLabel("Request Access");
    form.setEntrypoints(new ActionWorkflowEntrypointArray(entrypoint));

    trigger.setForm(form);
    workflowInfo.setTrigger(trigger);

    // Set steps with ONLY dynamic assignment (no static users)
    ActionWorkflowStep step = new ActionWorkflowStep();
    step.setId("step1");
    step.setType(ActionWorkflowStepType.APPROVAL);
    step.setDescription("First step");

    ActionWorkflowStepActors actors = new ActionWorkflowStepActors();
    // DO NOT set any static users - only dynamic assignment

    ActionWorkflowStepDynamicAssignment dynamicAssignment =
        new ActionWorkflowStepDynamicAssignment();
    dynamicAssignment.setType(ActionWorkflowStepDynamicAssignmentType.ENTITY_OWNERS);

    // Set ownership type URNs to filter by TECHNICAL_OWNER only
    UrnArray ownershipTypeUrns = new UrnArray();
    ownershipTypeUrns.add(UrnUtils.getUrn("urn:li:ownershipType:technical_owner"));
    dynamicAssignment.setOwnershipTypeUrns(ownershipTypeUrns);

    actors.setDynamicAssignment(dynamicAssignment);
    step.setActors(actors);

    workflowInfo.setSteps(new ActionWorkflowStepArray(step));

    return workflowInfo;
  }

  private ActionWorkflowInfo createTestWorkflowInfoWithoutOwnershipTypeFiltering() {
    ActionWorkflowInfo workflowInfo = new ActionWorkflowInfo();
    workflowInfo.setName("Test Workflow");
    workflowInfo.setCategory(ActionWorkflowCategory.ACCESS);
    workflowInfo.setDescription("Test workflow description");
    workflowInfo.setCustomCategory("custom-type");

    // Set audit stamps
    AuditStamp auditStamp =
        new AuditStamp().setTime(System.currentTimeMillis()).setActor(TEST_ACTOR_URN);
    workflowInfo.setCreated(auditStamp);
    workflowInfo.setLastModified(auditStamp);

    // Create trigger with form
    com.linkedin.actionworkflow.ActionWorkflowTrigger trigger =
        new com.linkedin.actionworkflow.ActionWorkflowTrigger();
    trigger.setType(com.linkedin.actionworkflow.ActionWorkflowTriggerType.FORM_SUBMITTED);

    com.linkedin.actionworkflow.ActionWorkflowForm form =
        new com.linkedin.actionworkflow.ActionWorkflowForm();

    // Set entity types in trigger form
    UrnArray entityTypes = new UrnArray();
    entityTypes.add(UrnUtils.getUrn("urn:li:entityType:dataset"));
    form.setEntityTypes(entityTypes);

    // Create fields array with field1 in trigger form
    ActionWorkflowFieldArray fields = new ActionWorkflowFieldArray();
    com.linkedin.actionworkflow.ActionWorkflowField field1 =
        new com.linkedin.actionworkflow.ActionWorkflowField();
    field1.setId("field1");
    field1.setName("Field 1");
    field1.setDescription("Test field 1");
    field1.setValueType(UrnUtils.getUrn("urn:li:structured:propertyType:string"));
    field1.setCardinality(com.linkedin.structured.PropertyCardinality.SINGLE);
    field1.setRequired(false);
    fields.add(field1);
    form.setFields(fields);

    // Set entrypoints in trigger form
    ActionWorkflowEntrypoint entrypoint = new ActionWorkflowEntrypoint();
    entrypoint.setType(ActionWorkflowEntrypointType.ENTITY_PROFILE);
    entrypoint.setLabel("Request Access");
    form.setEntrypoints(new ActionWorkflowEntrypointArray(entrypoint));

    trigger.setForm(form);
    workflowInfo.setTrigger(trigger);

    // Set steps with ONLY dynamic assignment (no static users)
    ActionWorkflowStep step = new ActionWorkflowStep();
    step.setId("step1");
    step.setType(ActionWorkflowStepType.APPROVAL);
    step.setDescription("First step");

    ActionWorkflowStepActors actors = new ActionWorkflowStepActors();
    // DO NOT set any static users - only dynamic assignment

    ActionWorkflowStepDynamicAssignment dynamicAssignment =
        new ActionWorkflowStepDynamicAssignment();
    dynamicAssignment.setType(ActionWorkflowStepDynamicAssignmentType.ENTITY_OWNERS);
    // No ownership type URNs specified - should return all owners

    actors.setDynamicAssignment(dynamicAssignment);
    step.setActors(actors);

    workflowInfo.setSteps(new ActionWorkflowStepArray(step));

    return workflowInfo;
  }

  private ActionWorkflowInfo createTestWorkflowInfoWithDomainOwnershipTypeFiltering() {
    ActionWorkflowInfo workflowInfo = new ActionWorkflowInfo();
    workflowInfo.setName("Test Workflow");
    workflowInfo.setCategory(ActionWorkflowCategory.ACCESS);
    workflowInfo.setDescription("Test workflow description");
    workflowInfo.setCustomCategory("custom-type");

    // Set audit stamps
    AuditStamp auditStamp =
        new AuditStamp().setTime(System.currentTimeMillis()).setActor(TEST_ACTOR_URN);
    workflowInfo.setCreated(auditStamp);
    workflowInfo.setLastModified(auditStamp);

    // Create trigger with form
    com.linkedin.actionworkflow.ActionWorkflowTrigger trigger =
        new com.linkedin.actionworkflow.ActionWorkflowTrigger();
    trigger.setType(com.linkedin.actionworkflow.ActionWorkflowTriggerType.FORM_SUBMITTED);

    com.linkedin.actionworkflow.ActionWorkflowForm form =
        new com.linkedin.actionworkflow.ActionWorkflowForm();

    // Set entity types in trigger form
    UrnArray entityTypes = new UrnArray();
    entityTypes.add(UrnUtils.getUrn("urn:li:entityType:dataset"));
    form.setEntityTypes(entityTypes);

    // Create fields array with field1 in trigger form
    ActionWorkflowFieldArray fields = new ActionWorkflowFieldArray();
    com.linkedin.actionworkflow.ActionWorkflowField field1 =
        new com.linkedin.actionworkflow.ActionWorkflowField();
    field1.setId("field1");
    field1.setName("Field 1");
    field1.setDescription("Test field 1");
    field1.setValueType(UrnUtils.getUrn("urn:li:structured:propertyType:string"));
    field1.setCardinality(com.linkedin.structured.PropertyCardinality.SINGLE);
    field1.setRequired(false);
    fields.add(field1);
    form.setFields(fields);

    // Set entrypoints in trigger form
    ActionWorkflowEntrypoint entrypoint = new ActionWorkflowEntrypoint();
    entrypoint.setType(ActionWorkflowEntrypointType.ENTITY_PROFILE);
    entrypoint.setLabel("Request Access");
    form.setEntrypoints(new ActionWorkflowEntrypointArray(entrypoint));

    trigger.setForm(form);
    workflowInfo.setTrigger(trigger);

    // Set steps with ONLY dynamic assignment (no static users)
    ActionWorkflowStep step = new ActionWorkflowStep();
    step.setId("step1");
    step.setType(ActionWorkflowStepType.APPROVAL);
    step.setDescription("First step");

    ActionWorkflowStepActors actors = new ActionWorkflowStepActors();
    // DO NOT set any static users - only dynamic assignment

    ActionWorkflowStepDynamicAssignment dynamicAssignment =
        new ActionWorkflowStepDynamicAssignment();
    dynamicAssignment.setType(ActionWorkflowStepDynamicAssignmentType.ENTITY_DOMAIN_OWNERS);

    // Set ownership type URNs to filter by BUSINESS_OWNER only
    UrnArray ownershipTypeUrns = new UrnArray();
    ownershipTypeUrns.add(UrnUtils.getUrn("urn:li:ownershipType:business_owner"));
    dynamicAssignment.setOwnershipTypeUrns(ownershipTypeUrns);

    actors.setDynamicAssignment(dynamicAssignment);
    step.setActors(actors);

    workflowInfo.setSteps(new ActionWorkflowStepArray(step));

    return workflowInfo;
  }

  private List<com.linkedin.common.Owner> createMixedOwnershipTypeOwners() {
    List<com.linkedin.common.Owner> owners = new ArrayList<>();

    // Owner with TECHNICAL_OWNER type
    com.linkedin.common.Owner technicalOwner = new com.linkedin.common.Owner();
    technicalOwner.setOwner(TEST_USER_URN);
    technicalOwner.setType(com.linkedin.common.OwnershipType.DATAOWNER);
    technicalOwner.setTypeUrn(UrnUtils.getUrn("urn:li:ownershipType:technical_owner"));
    owners.add(technicalOwner);

    // Owner with BUSINESS_OWNER type
    com.linkedin.common.Owner businessOwner = new com.linkedin.common.Owner();
    businessOwner.setOwner(UrnUtils.getUrn("urn:li:corpuser:businessuser"));
    businessOwner.setType(com.linkedin.common.OwnershipType.BUSINESS_OWNER);
    businessOwner.setTypeUrn(UrnUtils.getUrn("urn:li:ownershipType:business_owner"));
    owners.add(businessOwner);

    // Owner with no ownership type URN (should be excluded when filtering is active)
    com.linkedin.common.Owner ownerWithoutType = new com.linkedin.common.Owner();
    ownerWithoutType.setOwner(UrnUtils.getUrn("urn:li:corpuser:notypeuser"));
    ownerWithoutType.setType(com.linkedin.common.OwnershipType.DATAOWNER);
    // No typeUrn set - should be excluded when filtering
    owners.add(ownerWithoutType);

    return owners;
  }
}
