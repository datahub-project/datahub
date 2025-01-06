package com.linkedin.metadata.service;

import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.EDITABLE_DATASET_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_NODE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERM_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GROUP_MEMBERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.service.ActionRequestService.ACTION_REQUEST_RESULT_ACCEPTED;
import static com.linkedin.metadata.service.ActionRequestService.ACTION_REQUEST_STATUS_COMPLETE;
import static com.linkedin.metadata.service.ActionRequestService.ACTION_REQUEST_STATUS_PENDING;
import static com.linkedin.metadata.service.ActionRequestService.STRUCTURED_PROPERTY_ASSOCIATION_PROPOSAL_TYPE;
import static com.linkedin.metadata.service.ActionRequestService.TAG_ASSOCIATION_PROPOSAL_TYPE;
import static com.linkedin.metadata.service.ActionRequestService.TERM_ASSOCIATION_PROPOSAL_TYPE;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizedActors;
import com.datahub.authorization.EntitySpec;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestParams;
import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.actionrequest.GlossaryTermProposal;
import com.linkedin.actionrequest.StructuredPropertyProposal;
import com.linkedin.actionrequest.TagProposal;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.identity.GroupMembership;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.ActionRequestAspect;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.resource.SubResourceType;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.ActionRequestService.AlreadyAppliedException;
import com.linkedin.metadata.service.ActionRequestService.AlreadyRequestedException;
import com.linkedin.metadata.service.ActionRequestService.MalformedActionRequestException;
import com.linkedin.metadata.snapshot.ActionRequestSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import io.datahubproject.metadata.context.AuthorizationContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import java.net.URISyntaxException;
import java.util.*;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for {@link ActionRequestService}. */
public class ActionRequestServiceTest {

  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private SystemEntityClient mockEntityClient;
  private EntityService<?> mockEntityService;
  private GraphClient mockGraphClient;
  private DatasetService mockDatasetService;
  private TagService mockTagService;
  private GlossaryTermService mockGlossaryTermService;
  private StructuredPropertyService mockStructuredPropertyService;
  private OpenApiClient mockOpenApiClient;
  private ObjectMapper mockObjectMapper;
  private ActionRequestService actionRequestService;

  // Commonly used test fields
  private OperationContext mockOpContext;
  private AuthorizationContext mockAuthContext;
  private Authorizer mockAuthorizer;

  private final Urn TEST_ENTITY_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleDataset,PROD)");
  private final Urn TEST_TAG_URN = UrnUtils.getUrn("urn:li:tag:MyTestTag");
  private final Urn TEST_TAG_URN_2 = UrnUtils.getUrn("urn:li:tag:AnotherTag");
  private final Urn TEST_TERM_URN = UrnUtils.getUrn("urn:li:glossaryTerm:MyTestTerm");
  private final Urn TEST_TERM_URN_2 = UrnUtils.getUrn("urn:li:glossaryTerm:AnotherTerm");
  private final Urn TEST_STRUCTURED_PROPERTY_URN_1 =
      UrnUtils.getUrn("urn:li:structuredProperty:MyTestProp");
  private final Urn TEST_STRUCTURED_PROPERTY_URN_2 =
      UrnUtils.getUrn("urn:li:structuredProperty:MyTestProp2");
  static final String TEST_FIELD_PATH = "myColumn";
  private static final String GLOSSARY_NODE_NAME = "GLOSSARY_NODE";
  private static final String GLOSSARY_TERM_NAME = "GLOSSARY_TERM";
  private static final Urn ACTOR_URN = new CorpuserUrn("mock@email.com");
  private static final Urn GROUP_URN = new CorpGroupUrn("group");
  private static final String DESCRIPTION = "description";
  private static final Urn TEST_GLOSSARY_NODE_URN =
      UrnUtils.getUrn("urn:li:glossaryNode:12372c2ec7754c308993202dc44f548b");
  private static final Urn TEST_GLOSSARY_TERM_URN =
      UrnUtils.getUrn("urn:li:glossaryTerm:12372c2ec7754c308993202dc44f548b");

  @BeforeMethod
  public void setup() throws Exception {
    // Mocks
    mockEntityClient = mock(SystemEntityClient.class);
    mockEntityService = mock(EntityService.class);
    mockGraphClient = mock(GraphClient.class);
    mockDatasetService = mock(DatasetService.class);
    mockTagService = mock(TagService.class);
    mockGlossaryTermService = mock(GlossaryTermService.class);
    mockStructuredPropertyService = mock(StructuredPropertyService.class);
    mockOpenApiClient = mock(OpenApiClient.class);
    mockObjectMapper = mock(ObjectMapper.class);

    // OperationContext & AuthContext Mocks
    mockOpContext = mock(OperationContext.class);
    mockAuthContext = mock(AuthorizationContext.class);
    mockAuthorizer = mock(Authorizer.class);

    when(mockOpContext.getAuthorizationContext()).thenReturn(mockAuthContext);
    when(mockAuthContext.getAuthorizer()).thenReturn(mockAuthorizer);
    Authentication mockAuthentication = mock(Authentication.class);
    when(mockOpContext.getSessionAuthentication()).thenReturn(mockAuthentication);
    when(mockAuthentication.getActor())
        .thenReturn(new Actor(ActorType.USER, TEST_ACTOR_URN.getId()));

    // Default "no-op" behavior for manage entity tags, etc.
    AuthorizedActors mockAuthorizedActors =
        new AuthorizedActors(
            PoliciesConfig.MANAGE_ENTITY_TAGS_PRIVILEGE.getType(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            false,
            false);

    when(mockAuthorizer.authorizedActors(
            eq(PoliciesConfig.MANAGE_ENTITY_TAGS_PRIVILEGE.getType()), any(Optional.class)))
        .thenReturn(mockAuthorizedActors);

    when(mockAuthorizer.authorizedActors(
            eq(PoliciesConfig.MANAGE_DATASET_COL_TAGS_PRIVILEGE.getType()), any(Optional.class)))
        .thenReturn(mockAuthorizedActors);

    when(mockAuthorizer.authorizedActors(
            eq(PoliciesConfig.MANAGE_ENTITY_GLOSSARY_TERMS_PRIVILEGE.getType()),
            any(Optional.class)))
        .thenReturn(mockAuthorizedActors);

    when(mockAuthorizer.authorizedActors(
            eq(PoliciesConfig.MANAGE_DATASET_COL_GLOSSARY_TERMS_PRIVILEGE.getType()),
            any(Optional.class)))
        .thenReturn(mockAuthorizedActors);

    when(mockAuthorizer.authorizedActors(
            eq(PoliciesConfig.MANAGE_ENTITY_PROPERTIES_PRIVILEGE.getType()), any(Optional.class)))
        .thenReturn(mockAuthorizedActors);

    when(mockAuthorizer.authorizedActors(
            eq(PoliciesConfig.MANAGE_DATASET_COL_PROPERTIES_PRIVILEGE.getType()),
            any(Optional.class)))
        .thenReturn(mockAuthorizedActors);

    // Minimal default stubbing
    when(mockEntityClient.exists(any(OperationContext.class), any(Urn.class), anyBoolean()))
        .thenReturn(true);
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), anyBoolean()))
        .thenReturn(true);
    when(mockDatasetService.schemaFieldExists(
            any(OperationContext.class), any(Urn.class), anyString()))
        .thenReturn(true); // By default, assume schema field exists.
    when(mockStructuredPropertyService.areProposedStructuredPropertyValuesValid(
            eq(mockOpContext), any(), any()))
        .thenReturn(true);

    // Mock a default empty search result
    SearchResult mockSearchResult = new SearchResult();
    mockSearchResult.setEntities(new SearchEntityArray(Collections.emptyList()));
    when(mockEntityClient.filter(
            any(OperationContext.class),
            eq(Constants.ACTION_REQUEST_ENTITY_NAME),
            any(Filter.class),
            any(),
            anyInt(),
            anyInt()))
        .thenReturn(mockSearchResult);

    // Provide a default auditStamp
    AuditStamp defaultAuditStamp =
        new AuditStamp().setTime(System.currentTimeMillis()).setActor(TEST_ACTOR_URN);
    when(mockOpContext.getAuditStamp()).thenReturn(defaultAuditStamp);

    // Create the service to test
    actionRequestService =
        new ActionRequestService(
            mockEntityClient,
            mockEntityService,
            mockGraphClient,
            mockDatasetService,
            mockTagService,
            mockGlossaryTermService,
            mockStructuredPropertyService,
            mockOpenApiClient,
            mockObjectMapper);
  }

  // ============================================================================
  // TESTS FOR proposeEntityTags(...)
  // ============================================================================

  @Test
  public void testProposeEntityTagsSuccess() throws Exception {
    // Given
    when(mockTagService.getEntityTags(eq(mockOpContext), eq(TEST_ENTITY_URN)))
        .thenReturn(Collections.emptyList()); // No tags currently applied

    // When
    Urn result =
        actionRequestService.proposeEntityTags(
            mockOpContext, TEST_ENTITY_URN, Collections.singletonList(TEST_TAG_URN));

    // Then
    assertNotNull(result, "Returned URN from proposeEntityTags should not be null.");

    // 1) Capture the proposals passed to batchIngestProposals
    ArgumentCaptor<List<MetadataChangeProposal>> batchMcpCaptor =
        ArgumentCaptor.forClass((Class) List.class);
    verify(mockEntityClient, times(1))
        .batchIngestProposals(eq(mockOpContext), batchMcpCaptor.capture(), eq(false));
    List<MetadataChangeProposal> batchMcps = batchMcpCaptor.getValue();

    assertEquals(batchMcps.size(), 2, "Expect 2 MCPs for ActionRequestInfo + ActionRequestStatus");

    // Validate aspects
    MetadataChangeProposal infoProposal = batchMcps.get(0);
    MetadataChangeProposal statusProposal = batchMcps.get(1);

    assertEquals(infoProposal.getAspectName(), Constants.ACTION_REQUEST_INFO_ASPECT_NAME);
    assertEquals(statusProposal.getAspectName(), Constants.ACTION_REQUEST_STATUS_ASPECT_NAME);

    // Check that the entityUrn is an actionRequest URN
    assertEquals(infoProposal.getEntityUrn().getEntityType(), Constants.ACTION_REQUEST_ENTITY_NAME);

    // Deserialize & check ActionRequestInfo
    ActionRequestInfo info =
        GenericRecordUtils.deserializeAspect(
            infoProposal.getAspect().getValue(),
            infoProposal.getAspect().getContentType(),
            ActionRequestInfo.class);
    assertEquals(info.getType(), TAG_ASSOCIATION_PROPOSAL_TYPE);
    assertTrue(info.getParams().getTagProposal().hasTags());
    assertEquals(info.getParams().getTagProposal().getTags().size(), 1);
    assertEquals(info.getParams().getTagProposal().getTags().get(0), TEST_TAG_URN);

    // Deserialize & check ActionRequestStatus
    ActionRequestStatus status =
        GenericRecordUtils.deserializeAspect(
            statusProposal.getAspect().getValue(),
            statusProposal.getAspect().getContentType(),
            ActionRequestStatus.class);
    assertEquals(status.getStatus(), ACTION_REQUEST_STATUS_PENDING);

    // 2) Capture the single "ingestProposal" for updating the entity's proposals aspect
    ArgumentCaptor<MetadataChangeProposal> ingestProposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockEntityClient, times(1))
        .ingestProposal(eq(mockOpContext), ingestProposalCaptor.capture(), eq(false));
    MetadataChangeProposal proposalsMcp = ingestProposalCaptor.getValue();

    // Confirm it's about the entity proposals aspect
    assertEquals(proposalsMcp.getEntityUrn(), TEST_ENTITY_URN);
    assertEquals(proposalsMcp.getAspectName(), Constants.ENTITY_PROPOSALS_ASPECT_NAME);
  }

  @Test(expectedExceptions = EntityDoesNotExistException.class)
  public void testProposeEntityTagsEntityDoesNotExist() throws Exception {
    // Given
    when(mockEntityClient.exists(eq(mockOpContext), eq(TEST_ENTITY_URN), eq(false)))
        .thenReturn(false);

    // When
    actionRequestService.proposeEntityTags(
        mockOpContext, TEST_ENTITY_URN, Collections.singletonList(TEST_TAG_URN));
  }

  @Test(expectedExceptions = EntityDoesNotExistException.class)
  public void testProposeEntityTagsTagDoesNotExist() throws Exception {
    // Given
    when(mockEntityClient.exists(eq(mockOpContext), eq(TEST_TAG_URN), eq(false))).thenReturn(false);

    // When
    actionRequestService.proposeEntityTags(
        mockOpContext, TEST_ENTITY_URN, Collections.singletonList(TEST_TAG_URN));
  }

  // =======================
  // Filtering-Focused Tests
  // =======================

  @Test
  public void testProposeEntityTagsSomeAlreadyApplied() throws Exception {
    // Suppose we have 2 tags: TEST_TAG_URN and TEST_TAG_URN_2
    // One is already applied, the other is new
    TagAssociation alreadyAppliedTag =
        new TagAssociation().setTag(TagUrn.createFromUrn(TEST_TAG_URN));
    when(mockTagService.getEntityTags(eq(mockOpContext), eq(TEST_ENTITY_URN)))
        .thenReturn(Collections.singletonList(alreadyAppliedTag));

    // No previously proposed tags
    List<Urn> requestedTags = Arrays.asList(TEST_TAG_URN, TEST_TAG_URN_2);

    // Expect success, but only the new tag gets proposed
    Urn result =
        actionRequestService.proposeEntityTags(mockOpContext, TEST_ENTITY_URN, requestedTags);
    assertNotNull(result);

    // Capture the batchIngestProposals
    ArgumentCaptor<List<MetadataChangeProposal>> batchMcpCaptor =
        ArgumentCaptor.forClass((Class) List.class);
    verify(mockEntityClient, times(1))
        .batchIngestProposals(eq(mockOpContext), batchMcpCaptor.capture(), eq(false));
    List<MetadataChangeProposal> batchMcps = batchMcpCaptor.getValue();

    // The first MCP should be the ActionRequestInfo
    MetadataChangeProposal infoProposal = batchMcps.get(0);
    ActionRequestInfo info =
        GenericRecordUtils.deserializeAspect(
            infoProposal.getAspect().getValue(),
            infoProposal.getAspect().getContentType(),
            ActionRequestInfo.class);
    List<Urn> tagsInProposal = info.getParams().getTagProposal().getTags();
    assertEquals(tagsInProposal.size(), 1, "Should only propose the new tag");
    assertEquals(tagsInProposal.get(0), TEST_TAG_URN_2);
  }

  @Test(expectedExceptions = AlreadyAppliedException.class)
  public void testProposeEntityTagsAllApplied() throws Exception {
    // Both tags are already applied
    TagAssociation tag1 = new TagAssociation().setTag(TagUrn.createFromUrn(TEST_TAG_URN));
    TagAssociation tag2 = new TagAssociation().setTag(TagUrn.createFromUrn(TEST_TAG_URN_2));
    when(mockTagService.getEntityTags(eq(mockOpContext), eq(TEST_ENTITY_URN)))
        .thenReturn(Arrays.asList(tag1, tag2));

    // No previously proposed tags
    actionRequestService.proposeEntityTags(
        mockOpContext, TEST_ENTITY_URN, Arrays.asList(TEST_TAG_URN, TEST_TAG_URN_2));
  }

  @Test(expectedExceptions = AlreadyRequestedException.class)
  public void testProposeEntityTagsAllRequested() throws Exception {
    // Suppose no tags are actually applied, but both tags are already proposed
    when(mockTagService.getEntityTags(eq(mockOpContext), eq(TEST_ENTITY_URN)))
        .thenReturn(Collections.emptyList());

    // Stub the search to return an action request with both tags
    SearchResult mockSearchResult = new SearchResult();
    SearchEntity searchEntity = new SearchEntity();
    Urn existingActionRequestUrn = Urn.createFromString("urn:li:actionRequest:123");
    searchEntity.setEntity(existingActionRequestUrn);
    mockSearchResult.setEntities(new SearchEntityArray(Collections.singletonList(searchEntity)));
    when(mockEntityClient.filter(
            any(OperationContext.class),
            eq(Constants.ACTION_REQUEST_ENTITY_NAME),
            any(Filter.class),
            any(),
            anyInt(),
            anyInt()))
        .thenReturn(mockSearchResult);

    EntityResponse mockEntityResponse = mock(EntityResponse.class);
    Map<Urn, EntityResponse> mockBatchGetMap = new HashMap<>();
    mockBatchGetMap.put(existingActionRequestUrn, mockEntityResponse);

    when(mockEntityClient.batchGetV2(
            eq(mockOpContext), eq(Constants.ACTION_REQUEST_ENTITY_NAME), anySet(), anySet()))
        .thenReturn(mockBatchGetMap);

    // The existing action request has both tags
    ActionRequestInfo existingActionRequestInfo = new ActionRequestInfo();
    TagProposal tagProposal = new TagProposal();
    tagProposal.setTags(new UrnArray(Arrays.asList(TEST_TAG_URN, TEST_TAG_URN_2)));
    ActionRequestParams params = new ActionRequestParams().setTagProposal(tagProposal);
    existingActionRequestInfo.setParams(params);

    ActionRequestStatus existingActionRequestStatus =
        new ActionRequestStatus().setStatus(ACTION_REQUEST_STATUS_PENDING);

    when(mockEntityResponse.hasAspects()).thenReturn(true);
    when(mockEntityResponse.getAspects())
        .thenReturn(
            new EnvelopedAspectMap(
                ActionRequestTestUtils.mockAspectsMapForActionRequest(
                    existingActionRequestInfo, existingActionRequestStatus)));

    // Now propose both tags
    actionRequestService.proposeEntityTags(
        mockOpContext, TEST_ENTITY_URN, Arrays.asList(TEST_TAG_URN, TEST_TAG_URN_2));
  }

  // ============================================================================
  // TESTS FOR proposeSchemaFieldTags(...)
  // ============================================================================

  @Test
  public void testProposeSchemaFieldTagsSuccess() throws Exception {
    // Given
    when(mockTagService.getSchemaFieldTags(
            eq(mockOpContext), eq(TEST_ENTITY_URN), eq(TEST_FIELD_PATH)))
        .thenReturn(Collections.emptyList());

    // When
    Urn result =
        actionRequestService.proposeSchemaFieldTags(
            mockOpContext,
            TEST_ENTITY_URN,
            TEST_FIELD_PATH,
            Collections.singletonList(TEST_TAG_URN));

    // Then
    assertNotNull(result);

    // 1) Capture the proposals passed to batchIngestProposals
    ArgumentCaptor<List<MetadataChangeProposal>> batchMcpCaptor =
        ArgumentCaptor.forClass((Class) List.class);
    verify(mockEntityClient, times(1))
        .batchIngestProposals(eq(mockOpContext), batchMcpCaptor.capture(), eq(false));
    List<MetadataChangeProposal> batchMcps = batchMcpCaptor.getValue();

    assertEquals(batchMcps.size(), 2, "Expect 2 MCPs for ActionRequestInfo + ActionRequestStatus");

    MetadataChangeProposal infoProposal = batchMcps.get(0);
    MetadataChangeProposal statusProposal = batchMcps.get(1);

    // Check aspect names
    assertEquals(infoProposal.getAspectName(), Constants.ACTION_REQUEST_INFO_ASPECT_NAME);
    assertEquals(statusProposal.getAspectName(), Constants.ACTION_REQUEST_STATUS_ASPECT_NAME);

    // Confirm actionRequest entity type
    assertEquals(infoProposal.getEntityUrn().getEntityType(), Constants.ACTION_REQUEST_ENTITY_NAME);

    // Validate info aspect
    ActionRequestInfo info =
        GenericRecordUtils.deserializeAspect(
            infoProposal.getAspect().getValue(),
            infoProposal.getAspect().getContentType(),
            ActionRequestInfo.class);
    assertEquals(info.getType(), TAG_ASSOCIATION_PROPOSAL_TYPE);
    assertEquals(info.getSubResourceType(), SubResourceType.DATASET_FIELD.toString());
    assertEquals(info.getSubResource(), TEST_FIELD_PATH);
    assertTrue(info.getParams().getTagProposal().hasTags());
    assertEquals(info.getParams().getTagProposal().getTags().get(0), TEST_TAG_URN);

    // Validate status aspect
    ActionRequestStatus status =
        GenericRecordUtils.deserializeAspect(
            statusProposal.getAspect().getValue(),
            statusProposal.getAspect().getContentType(),
            ActionRequestStatus.class);
    assertEquals(status.getStatus(), ACTION_REQUEST_STATUS_PENDING);

    // 2) Capture the single "ingestProposal"
    ArgumentCaptor<MetadataChangeProposal> ingestProposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockEntityClient, times(1))
        .ingestProposal(eq(mockOpContext), ingestProposalCaptor.capture(), eq(false));
    MetadataChangeProposal proposalsMcp = ingestProposalCaptor.getValue();

    // Confirm it's about dataset_schema_proposals
    assertEquals(proposalsMcp.getEntityUrn(), TEST_ENTITY_URN);
    assertEquals(proposalsMcp.getAspectName(), Constants.DATASET_SCHEMA_PROPOSALS_ASPECT_NAME);
  }

  @Test(expectedExceptions = EntityDoesNotExistException.class)
  public void testProposeSchemaFieldTagsEntityDoesNotExist() throws Exception {
    // Given
    when(mockEntityClient.exists(eq(mockOpContext), eq(TEST_ENTITY_URN), eq(false)))
        .thenReturn(false);

    // When
    actionRequestService.proposeSchemaFieldTags(
        mockOpContext, TEST_ENTITY_URN, TEST_FIELD_PATH, Collections.singletonList(TEST_TAG_URN));
  }

  @Test(expectedExceptions = EntityDoesNotExistException.class)
  public void testProposeSchemaFieldTagsFieldPathDoesNotExist() throws Exception {
    // Given
    when(mockDatasetService.schemaFieldExists(
            eq(mockOpContext), eq(TEST_ENTITY_URN), eq(TEST_FIELD_PATH)))
        .thenReturn(false);

    // When
    actionRequestService.proposeSchemaFieldTags(
        mockOpContext, TEST_ENTITY_URN, TEST_FIELD_PATH, Collections.singletonList(TEST_TAG_URN));
  }

  @Test(expectedExceptions = EntityDoesNotExistException.class)
  public void testProposeSchemaFieldTagsTagDoesNotExist() throws Exception {
    // Given
    when(mockEntityClient.exists(eq(mockOpContext), eq(TEST_TAG_URN), eq(false))).thenReturn(false);

    // When
    actionRequestService.proposeSchemaFieldTags(
        mockOpContext, TEST_ENTITY_URN, TEST_FIELD_PATH, Collections.singletonList(TEST_TAG_URN));
  }

  @Test(expectedExceptions = AlreadyAppliedException.class)
  public void testProposeSchemaFieldTagsAlreadyApplied() throws Exception {
    // Given
    TagAssociation tagAssociation = new TagAssociation().setTag(TagUrn.createFromUrn(TEST_TAG_URN));
    when(mockTagService.getSchemaFieldTags(
            eq(mockOpContext), eq(TEST_ENTITY_URN), eq(TEST_FIELD_PATH)))
        .thenReturn(Collections.singletonList(tagAssociation));

    // When
    actionRequestService.proposeSchemaFieldTags(
        mockOpContext, TEST_ENTITY_URN, TEST_FIELD_PATH, Collections.singletonList(TEST_TAG_URN));
  }

  @Test(expectedExceptions = AlreadyRequestedException.class)
  public void testProposeSchemaFieldTagsAlreadyRequested() throws Exception {
    // Given
    SearchResult mockSearchResult = new SearchResult();
    SearchEntity searchEntity = new SearchEntity();
    Urn existingActionRequestUrn = Urn.createFromString("urn:li:actionRequest:XYZ");
    searchEntity.setEntity(existingActionRequestUrn);
    mockSearchResult.setEntities(new SearchEntityArray(Collections.singletonList(searchEntity)));

    when(mockEntityClient.filter(
            any(OperationContext.class),
            eq(Constants.ACTION_REQUEST_ENTITY_NAME),
            any(Filter.class),
            any(),
            anyInt(),
            anyInt()))
        .thenReturn(mockSearchResult);

    EntityResponse mockEntityResponse = mock(EntityResponse.class);
    Map<Urn, EntityResponse> mockBatchGetMap = new HashMap<>();
    mockBatchGetMap.put(existingActionRequestUrn, mockEntityResponse);

    when(mockEntityClient.batchGetV2(
            eq(mockOpContext), eq(Constants.ACTION_REQUEST_ENTITY_NAME), anySet(), anySet()))
        .thenReturn(mockBatchGetMap);

    ActionRequestInfo existingActionRequestInfo = new ActionRequestInfo();
    TagProposal tagProposal = new TagProposal();
    tagProposal.setTags(new UrnArray(Collections.singletonList(TEST_TAG_URN)));
    ActionRequestParams params = new ActionRequestParams().setTagProposal(tagProposal);
    existingActionRequestInfo.setParams(params);
    existingActionRequestInfo.setSubResourceType(SubResourceType.DATASET_FIELD.toString());
    existingActionRequestInfo.setSubResource(TEST_FIELD_PATH);

    ActionRequestStatus existingActionRequestStatus = new ActionRequestStatus();
    existingActionRequestStatus.setStatus(ACTION_REQUEST_STATUS_PENDING);

    when(mockEntityResponse.hasAspects()).thenReturn(true);
    when(mockEntityResponse.getAspects())
        .thenReturn(
            new EnvelopedAspectMap(
                ActionRequestTestUtils.mockAspectsMapForActionRequest(
                    existingActionRequestInfo, existingActionRequestStatus)));

    // When
    actionRequestService.proposeSchemaFieldTags(
        mockOpContext, TEST_ENTITY_URN, TEST_FIELD_PATH, Collections.singletonList(TEST_TAG_URN));
  }

  // =======================
  // Filtering-Focused Tests
  // =======================

  @Test
  public void testProposeSchemaFieldTagsSomeAlreadyApplied() throws Exception {
    // Suppose we have 2 tags: TEST_TAG_URN and TEST_TAG_URN_2
    TagAssociation alreadyApplied = new TagAssociation().setTag(TagUrn.createFromUrn(TEST_TAG_URN));
    when(mockTagService.getSchemaFieldTags(
            eq(mockOpContext), eq(TEST_ENTITY_URN), eq(TEST_FIELD_PATH)))
        .thenReturn(Collections.singletonList(alreadyApplied));

    // No previously proposed
    List<Urn> requestedTags = Arrays.asList(TEST_TAG_URN, TEST_TAG_URN_2);

    // Expect success, only the new tag (TEST_TAG_URN_2) is proposed
    Urn result =
        actionRequestService.proposeSchemaFieldTags(
            mockOpContext, TEST_ENTITY_URN, TEST_FIELD_PATH, requestedTags);
    assertNotNull(result);

    // Check we propose exactly 1 new tag
    ArgumentCaptor<List<MetadataChangeProposal>> batchMcpCaptor =
        ArgumentCaptor.forClass((Class) List.class);
    verify(mockEntityClient, times(1))
        .batchIngestProposals(eq(mockOpContext), batchMcpCaptor.capture(), eq(false));
    List<MetadataChangeProposal> batchMcps = batchMcpCaptor.getValue();

    MetadataChangeProposal infoProposal = batchMcps.get(0);
    ActionRequestInfo info =
        GenericRecordUtils.deserializeAspect(
            infoProposal.getAspect().getValue(),
            infoProposal.getAspect().getContentType(),
            ActionRequestInfo.class);
    assertEquals(info.getParams().getTagProposal().getTags().size(), 1);
    assertEquals(info.getParams().getTagProposal().getTags().get(0), TEST_TAG_URN_2);
  }

  @Test(expectedExceptions = AlreadyAppliedException.class)
  public void testProposeSchemaFieldTagsAllApplied() throws Exception {
    // Both tags already applied on the field
    TagAssociation tag1 = new TagAssociation().setTag(TagUrn.createFromUrn(TEST_TAG_URN));
    TagAssociation tag2 = new TagAssociation().setTag(TagUrn.createFromUrn(TEST_TAG_URN_2));
    when(mockTagService.getSchemaFieldTags(
            eq(mockOpContext), eq(TEST_ENTITY_URN), eq(TEST_FIELD_PATH)))
        .thenReturn(Arrays.asList(tag1, tag2));

    actionRequestService.proposeSchemaFieldTags(
        mockOpContext,
        TEST_ENTITY_URN,
        TEST_FIELD_PATH,
        Arrays.asList(TEST_TAG_URN, TEST_TAG_URN_2));
  }

  @Test(expectedExceptions = AlreadyRequestedException.class)
  public void testProposeSchemaFieldTagsAllRequested() throws Exception {
    // None are actually applied, but both tags are already requested
    when(mockTagService.getSchemaFieldTags(
            eq(mockOpContext), eq(TEST_ENTITY_URN), eq(TEST_FIELD_PATH)))
        .thenReturn(Collections.emptyList());

    // Stub the search to return an existing action request with both tags
    SearchResult mockSearchResult = new SearchResult();
    SearchEntity searchEntity = new SearchEntity();
    Urn existingActionRequestUrn = Urn.createFromString("urn:li:actionRequest:XYZ");
    searchEntity.setEntity(existingActionRequestUrn);
    mockSearchResult.setEntities(new SearchEntityArray(Collections.singletonList(searchEntity)));
    when(mockEntityClient.filter(
            any(OperationContext.class),
            eq(Constants.ACTION_REQUEST_ENTITY_NAME),
            any(Filter.class),
            any(),
            anyInt(),
            anyInt()))
        .thenReturn(mockSearchResult);

    EntityResponse mockEntityResponse = mock(EntityResponse.class);
    Map<Urn, EntityResponse> mockBatchGetMap = new HashMap<>();
    mockBatchGetMap.put(existingActionRequestUrn, mockEntityResponse);

    when(mockEntityClient.batchGetV2(
            eq(mockOpContext), eq(Constants.ACTION_REQUEST_ENTITY_NAME), anySet(), anySet()))
        .thenReturn(mockBatchGetMap);

    ActionRequestInfo existingActionRequestInfo = new ActionRequestInfo();
    TagProposal tagProposal = new TagProposal();
    tagProposal.setTags(new UrnArray(Arrays.asList(TEST_TAG_URN, TEST_TAG_URN_2)));
    ActionRequestParams params = new ActionRequestParams().setTagProposal(tagProposal);
    existingActionRequestInfo.setParams(params);
    existingActionRequestInfo.setSubResource(TEST_FIELD_PATH);
    existingActionRequestInfo.setSubResourceType(SubResourceType.DATASET_FIELD.toString());

    ActionRequestStatus existingActionRequestStatus =
        new ActionRequestStatus().setStatus(ACTION_REQUEST_STATUS_PENDING);

    when(mockEntityResponse.hasAspects()).thenReturn(true);
    when(mockEntityResponse.getAspects())
        .thenReturn(
            new EnvelopedAspectMap(
                ActionRequestTestUtils.mockAspectsMapForActionRequest(
                    existingActionRequestInfo, existingActionRequestStatus)));

    // Now propose both tags
    actionRequestService.proposeSchemaFieldTags(
        mockOpContext,
        TEST_ENTITY_URN,
        TEST_FIELD_PATH,
        Arrays.asList(TEST_TAG_URN, TEST_TAG_URN_2));
  }

  @Test(expectedExceptions = MalformedActionRequestException.class)
  public void testProposeSchemaFieldTagsEntityNotDataset() throws Exception {
    // Given
    Urn someOtherEntityUrn = Urn.createFromString("urn:li:chart:123");

    // When
    actionRequestService.proposeSchemaFieldTags(
        mockOpContext,
        someOtherEntityUrn,
        TEST_FIELD_PATH,
        Collections.singletonList(TEST_TAG_URN));
  }

  // ============================================================================
  // TESTS FOR proposeEntityTerms(...)
  // ============================================================================

  @Test
  public void testProposeEntityTermsSuccess() throws Exception {
    // Given
    when(mockGlossaryTermService.getEntityTerms(eq(mockOpContext), eq(TEST_ENTITY_URN)))
        .thenReturn(Collections.emptyList()); // No tags currently applied

    // When
    Urn result =
        actionRequestService.proposeEntityTerms(
            mockOpContext, TEST_ENTITY_URN, Collections.singletonList(TEST_TAG_URN));

    // Then
    assertNotNull(result, "Returned URN from proposeEntityTerms should not be null.");

    // 1) Capture the proposals passed to batchIngestProposals
    ArgumentCaptor<List<MetadataChangeProposal>> batchMcpCaptor =
        ArgumentCaptor.forClass((Class) List.class);
    verify(mockEntityClient, times(1))
        .batchIngestProposals(eq(mockOpContext), batchMcpCaptor.capture(), eq(false));
    List<MetadataChangeProposal> batchMcps = batchMcpCaptor.getValue();

    assertEquals(batchMcps.size(), 2, "Expect 2 MCPs for ActionRequestInfo + ActionRequestStatus");

    // Validate aspects
    MetadataChangeProposal infoProposal = batchMcps.get(0);
    MetadataChangeProposal statusProposal = batchMcps.get(1);

    assertEquals(infoProposal.getAspectName(), Constants.ACTION_REQUEST_INFO_ASPECT_NAME);
    assertEquals(statusProposal.getAspectName(), Constants.ACTION_REQUEST_STATUS_ASPECT_NAME);

    // Check that the entityUrn is an actionRequest URN
    assertEquals(infoProposal.getEntityUrn().getEntityType(), Constants.ACTION_REQUEST_ENTITY_NAME);

    // Deserialize & check ActionRequestInfo
    ActionRequestInfo info =
        GenericRecordUtils.deserializeAspect(
            infoProposal.getAspect().getValue(),
            infoProposal.getAspect().getContentType(),
            ActionRequestInfo.class);
    assertEquals(info.getType(), TERM_ASSOCIATION_PROPOSAL_TYPE);
    assertTrue(info.getParams().getGlossaryTermProposal().hasGlossaryTerms());
    assertEquals(info.getParams().getGlossaryTermProposal().getGlossaryTerms().size(), 1);
    assertEquals(
        info.getParams().getGlossaryTermProposal().getGlossaryTerms().get(0), TEST_TAG_URN);

    // Deserialize & check ActionRequestStatus
    ActionRequestStatus status =
        GenericRecordUtils.deserializeAspect(
            statusProposal.getAspect().getValue(),
            statusProposal.getAspect().getContentType(),
            ActionRequestStatus.class);
    assertEquals(status.getStatus(), ACTION_REQUEST_STATUS_PENDING);

    // 2) Capture the single "ingestProposal" for updating the entity's proposals aspect
    ArgumentCaptor<MetadataChangeProposal> ingestProposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockEntityClient, times(1))
        .ingestProposal(eq(mockOpContext), ingestProposalCaptor.capture(), eq(false));
    MetadataChangeProposal proposalsMcp = ingestProposalCaptor.getValue();

    // Confirm it's about the entity proposals aspect
    assertEquals(proposalsMcp.getEntityUrn(), TEST_ENTITY_URN);
    assertEquals(proposalsMcp.getAspectName(), Constants.ENTITY_PROPOSALS_ASPECT_NAME);
  }

  @Test(expectedExceptions = EntityDoesNotExistException.class)
  public void testProposeEntityTermsEntityDoesNotExist() throws Exception {
    // Given
    when(mockEntityClient.exists(eq(mockOpContext), eq(TEST_ENTITY_URN), eq(false)))
        .thenReturn(false);

    // When
    actionRequestService.proposeEntityTerms(
        mockOpContext, TEST_ENTITY_URN, Collections.singletonList(TEST_TAG_URN));
  }

  @Test(expectedExceptions = EntityDoesNotExistException.class)
  public void testProposeEntityTermsTermDoesNotExist() throws Exception {
    // Given
    when(mockEntityClient.exists(eq(mockOpContext), eq(TEST_TAG_URN), eq(false))).thenReturn(false);

    // When
    actionRequestService.proposeEntityTerms(
        mockOpContext, TEST_ENTITY_URN, Collections.singletonList(TEST_TAG_URN));
  }

  // =======================
  // Filtering-Focused Tests
  // =======================

  @Test
  public void testProposeEntityTermsSomeAlreadyApplied() throws Exception {
    // Suppose we have 2 tags: TEST_TAG_URN and TEST_TAG_URN_2
    // One is already applied, the other is new
    GlossaryTermAssociation alreadyAppliedTerm =
        new GlossaryTermAssociation().setUrn(GlossaryTermUrn.createFromUrn(TEST_TERM_URN));
    when(mockGlossaryTermService.getEntityTerms(eq(mockOpContext), eq(TEST_ENTITY_URN)))
        .thenReturn(Collections.singletonList(alreadyAppliedTerm));

    // No previously proposed terms
    List<Urn> requestedTerms = Arrays.asList(TEST_TERM_URN, TEST_TERM_URN_2);

    // Expect success, but only the new term gets proposed
    Urn result =
        actionRequestService.proposeEntityTerms(mockOpContext, TEST_ENTITY_URN, requestedTerms);
    assertNotNull(result);

    // Capture the batchIngestProposals
    ArgumentCaptor<List<MetadataChangeProposal>> batchMcpCaptor =
        ArgumentCaptor.forClass((Class) List.class);
    verify(mockEntityClient, times(1))
        .batchIngestProposals(eq(mockOpContext), batchMcpCaptor.capture(), eq(false));
    List<MetadataChangeProposal> batchMcps = batchMcpCaptor.getValue();

    // The first MCP should be the ActionRequestInfo
    MetadataChangeProposal infoProposal = batchMcps.get(0);
    ActionRequestInfo info =
        GenericRecordUtils.deserializeAspect(
            infoProposal.getAspect().getValue(),
            infoProposal.getAspect().getContentType(),
            ActionRequestInfo.class);
    List<Urn> termsInProposal = info.getParams().getGlossaryTermProposal().getGlossaryTerms();
    assertEquals(termsInProposal.size(), 1, "Should only propose the new term");
    assertEquals(termsInProposal.get(0), TEST_TERM_URN_2);
  }

  @Test(expectedExceptions = AlreadyAppliedException.class)
  public void testProposeEntityTermsAllApplied() throws Exception {
    // Both terms are already applied
    GlossaryTermAssociation term1 =
        new GlossaryTermAssociation().setUrn(GlossaryTermUrn.createFromUrn(TEST_TERM_URN));
    GlossaryTermAssociation term2 =
        new GlossaryTermAssociation().setUrn(GlossaryTermUrn.createFromUrn(TEST_TERM_URN_2));
    when(mockGlossaryTermService.getEntityTerms(eq(mockOpContext), eq(TEST_ENTITY_URN)))
        .thenReturn(Arrays.asList(term1, term2));

    // No previously proposed terms
    actionRequestService.proposeEntityTerms(
        mockOpContext, TEST_ENTITY_URN, Arrays.asList(TEST_TERM_URN, TEST_TERM_URN_2));
  }

  @Test(expectedExceptions = AlreadyRequestedException.class)
  public void testProposeEntityTermsAllRequested() throws Exception {
    // Suppose no tags are actually applied, but both terms are already proposed
    when(mockGlossaryTermService.getEntityTerms(eq(mockOpContext), eq(TEST_ENTITY_URN)))
        .thenReturn(Collections.emptyList());

    // Stub the search to return an action request with both terms
    SearchResult mockSearchResult = new SearchResult();
    SearchEntity searchEntity = new SearchEntity();
    Urn existingActionRequestUrn = Urn.createFromString("urn:li:actionRequest:123");
    searchEntity.setEntity(existingActionRequestUrn);
    mockSearchResult.setEntities(new SearchEntityArray(Collections.singletonList(searchEntity)));
    when(mockEntityClient.filter(
            any(OperationContext.class),
            eq(Constants.ACTION_REQUEST_ENTITY_NAME),
            any(Filter.class),
            any(),
            anyInt(),
            anyInt()))
        .thenReturn(mockSearchResult);

    EntityResponse mockEntityResponse = mock(EntityResponse.class);
    Map<Urn, EntityResponse> mockBatchGetMap = new HashMap<>();
    mockBatchGetMap.put(existingActionRequestUrn, mockEntityResponse);

    when(mockEntityClient.batchGetV2(
            eq(mockOpContext), eq(Constants.ACTION_REQUEST_ENTITY_NAME), anySet(), anySet()))
        .thenReturn(mockBatchGetMap);

    // The existing action request has both tags
    ActionRequestInfo existingActionRequestInfo = new ActionRequestInfo();
    GlossaryTermProposal termProposal = new GlossaryTermProposal();
    termProposal.setGlossaryTerms(new UrnArray(Arrays.asList(TEST_TERM_URN, TEST_TERM_URN_2)));
    ActionRequestParams params = new ActionRequestParams().setGlossaryTermProposal(termProposal);
    existingActionRequestInfo.setParams(params);

    ActionRequestStatus existingActionRequestStatus =
        new ActionRequestStatus().setStatus(ACTION_REQUEST_STATUS_PENDING);

    when(mockEntityResponse.hasAspects()).thenReturn(true);
    when(mockEntityResponse.getAspects())
        .thenReturn(
            new EnvelopedAspectMap(
                ActionRequestTestUtils.mockAspectsMapForActionRequest(
                    existingActionRequestInfo, existingActionRequestStatus)));

    // Now propose both tags
    actionRequestService.proposeEntityTerms(
        mockOpContext, TEST_ENTITY_URN, Arrays.asList(TEST_TERM_URN, TEST_TERM_URN_2));
  }

  // ============================================================================
  // TESTS FOR proposeSchemaFieldTerms(...)
  // ============================================================================

  @Test
  public void testProposeSchemaFieldTermsSuccess() throws Exception {
    // Given
    when(mockGlossaryTermService.getSchemaFieldTerms(
            eq(mockOpContext), eq(TEST_ENTITY_URN), eq(TEST_FIELD_PATH)))
        .thenReturn(Collections.emptyList());

    // When
    Urn result =
        actionRequestService.proposeSchemaFieldTerms(
            mockOpContext,
            TEST_ENTITY_URN,
            TEST_FIELD_PATH,
            Collections.singletonList(TEST_TERM_URN));

    // Then
    assertNotNull(result);

    // 1) Capture the proposals passed to batchIngestProposals
    ArgumentCaptor<List<MetadataChangeProposal>> batchMcpCaptor =
        ArgumentCaptor.forClass((Class) List.class);
    verify(mockEntityClient, times(1))
        .batchIngestProposals(eq(mockOpContext), batchMcpCaptor.capture(), eq(false));
    List<MetadataChangeProposal> batchMcps = batchMcpCaptor.getValue();

    assertEquals(batchMcps.size(), 2, "Expect 2 MCPs for ActionRequestInfo + ActionRequestStatus");

    MetadataChangeProposal infoProposal = batchMcps.get(0);
    MetadataChangeProposal statusProposal = batchMcps.get(1);

    // Check aspect names
    assertEquals(infoProposal.getAspectName(), Constants.ACTION_REQUEST_INFO_ASPECT_NAME);
    assertEquals(statusProposal.getAspectName(), Constants.ACTION_REQUEST_STATUS_ASPECT_NAME);

    // Confirm actionRequest entity type
    assertEquals(infoProposal.getEntityUrn().getEntityType(), Constants.ACTION_REQUEST_ENTITY_NAME);

    // Validate info aspect
    ActionRequestInfo info =
        GenericRecordUtils.deserializeAspect(
            infoProposal.getAspect().getValue(),
            infoProposal.getAspect().getContentType(),
            ActionRequestInfo.class);
    assertEquals(info.getType(), TERM_ASSOCIATION_PROPOSAL_TYPE);
    assertEquals(info.getSubResourceType(), SubResourceType.DATASET_FIELD.toString());
    assertEquals(info.getSubResource(), TEST_FIELD_PATH);
    assertTrue(info.getParams().getGlossaryTermProposal().hasGlossaryTerms());
    assertEquals(
        info.getParams().getGlossaryTermProposal().getGlossaryTerms().get(0), TEST_TERM_URN);

    // Validate status aspect
    ActionRequestStatus status =
        GenericRecordUtils.deserializeAspect(
            statusProposal.getAspect().getValue(),
            statusProposal.getAspect().getContentType(),
            ActionRequestStatus.class);
    assertEquals(status.getStatus(), ACTION_REQUEST_STATUS_PENDING);

    // 2) Capture the single "ingestProposal"
    ArgumentCaptor<MetadataChangeProposal> ingestProposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockEntityClient, times(1))
        .ingestProposal(eq(mockOpContext), ingestProposalCaptor.capture(), eq(false));
    MetadataChangeProposal proposalsMcp = ingestProposalCaptor.getValue();

    // Confirm it's a dataset schema proposal emission.
    assertEquals(proposalsMcp.getEntityUrn(), TEST_ENTITY_URN);
    assertEquals(proposalsMcp.getAspectName(), Constants.DATASET_SCHEMA_PROPOSALS_ASPECT_NAME);
  }

  @Test(expectedExceptions = EntityDoesNotExistException.class)
  public void testProposeSchemaFieldTermsEntityDoesNotExist() throws Exception {
    // Given
    when(mockEntityClient.exists(eq(mockOpContext), eq(TEST_ENTITY_URN), eq(false)))
        .thenReturn(false);

    // When
    actionRequestService.proposeSchemaFieldTerms(
        mockOpContext, TEST_ENTITY_URN, TEST_FIELD_PATH, Collections.singletonList(TEST_TERM_URN));
  }

  @Test(expectedExceptions = EntityDoesNotExistException.class)
  public void testProposeSchemaFieldTermsFieldPathDoesNotExist() throws Exception {
    // Given
    when(mockDatasetService.schemaFieldExists(
            eq(mockOpContext), eq(TEST_ENTITY_URN), eq(TEST_FIELD_PATH)))
        .thenReturn(false);

    // When
    actionRequestService.proposeSchemaFieldTerms(
        mockOpContext, TEST_ENTITY_URN, TEST_FIELD_PATH, Collections.singletonList(TEST_TERM_URN));
  }

  @Test(expectedExceptions = EntityDoesNotExistException.class)
  public void testProposeSchemaFieldTermsTermDoesNotExist() throws Exception {
    // Given
    when(mockEntityClient.exists(eq(mockOpContext), eq(TEST_TERM_URN), eq(false)))
        .thenReturn(false);

    // When
    actionRequestService.proposeSchemaFieldTerms(
        mockOpContext, TEST_ENTITY_URN, TEST_FIELD_PATH, Collections.singletonList(TEST_TERM_URN));
  }

  @Test(expectedExceptions = AlreadyAppliedException.class)
  public void testProposeSchemaFieldTermsAlreadyApplied() throws Exception {
    // Given
    GlossaryTermAssociation termAssociation =
        new GlossaryTermAssociation().setUrn(GlossaryTermUrn.createFromUrn(TEST_TERM_URN));
    when(mockGlossaryTermService.getSchemaFieldTerms(
            eq(mockOpContext), eq(TEST_ENTITY_URN), eq(TEST_FIELD_PATH)))
        .thenReturn(Collections.singletonList(termAssociation));

    // When
    actionRequestService.proposeSchemaFieldTerms(
        mockOpContext, TEST_ENTITY_URN, TEST_FIELD_PATH, Collections.singletonList(TEST_TERM_URN));
  }

  @Test(expectedExceptions = AlreadyRequestedException.class)
  public void testProposeSchemaFieldTermsAlreadyRequested() throws Exception {
    // Given
    SearchResult mockSearchResult = new SearchResult();
    SearchEntity searchEntity = new SearchEntity();
    Urn existingActionRequestUrn = Urn.createFromString("urn:li:actionRequest:XYZ");
    searchEntity.setEntity(existingActionRequestUrn);
    mockSearchResult.setEntities(new SearchEntityArray(Collections.singletonList(searchEntity)));

    when(mockEntityClient.filter(
            any(OperationContext.class),
            eq(Constants.ACTION_REQUEST_ENTITY_NAME),
            any(Filter.class),
            any(),
            anyInt(),
            anyInt()))
        .thenReturn(mockSearchResult);

    EntityResponse mockEntityResponse = mock(EntityResponse.class);
    Map<Urn, EntityResponse> mockBatchGetMap = new HashMap<>();
    mockBatchGetMap.put(existingActionRequestUrn, mockEntityResponse);

    when(mockEntityClient.batchGetV2(
            eq(mockOpContext), eq(Constants.ACTION_REQUEST_ENTITY_NAME), anySet(), anySet()))
        .thenReturn(mockBatchGetMap);

    ActionRequestInfo existingActionRequestInfo = new ActionRequestInfo();
    GlossaryTermProposal termProposal = new GlossaryTermProposal();
    termProposal.setGlossaryTerms(new UrnArray(Collections.singletonList(TEST_TERM_URN)));
    ActionRequestParams params = new ActionRequestParams().setGlossaryTermProposal(termProposal);
    existingActionRequestInfo.setParams(params);
    existingActionRequestInfo.setSubResourceType(SubResourceType.DATASET_FIELD.toString());
    existingActionRequestInfo.setSubResource(TEST_FIELD_PATH);

    ActionRequestStatus existingActionRequestStatus = new ActionRequestStatus();
    existingActionRequestStatus.setStatus(ACTION_REQUEST_STATUS_PENDING);

    when(mockEntityResponse.hasAspects()).thenReturn(true);
    when(mockEntityResponse.getAspects())
        .thenReturn(
            new EnvelopedAspectMap(
                ActionRequestTestUtils.mockAspectsMapForActionRequest(
                    existingActionRequestInfo, existingActionRequestStatus)));

    // When
    actionRequestService.proposeSchemaFieldTerms(
        mockOpContext, TEST_ENTITY_URN, TEST_FIELD_PATH, Collections.singletonList(TEST_TERM_URN));
  }

  // =======================
  // Filtering-Focused Tests
  // =======================

  @Test
  public void testProposeSchemaFieldTermsSomeAlreadyApplied() throws Exception {
    // Suppose we have 2 terms: TEST_TERM_URN and TEST_TERM_URN_2
    GlossaryTermAssociation alreadyApplied =
        new GlossaryTermAssociation().setUrn(GlossaryTermUrn.createFromUrn(TEST_TERM_URN));
    when(mockGlossaryTermService.getSchemaFieldTerms(
            eq(mockOpContext), eq(TEST_ENTITY_URN), eq(TEST_FIELD_PATH)))
        .thenReturn(Collections.singletonList(alreadyApplied));

    // No previously proposed
    List<Urn> requestedTags = Arrays.asList(TEST_TERM_URN, TEST_TERM_URN_2);

    // Expect success, only the new tag (TEST_TERM_URN_2) is proposed
    Urn result =
        actionRequestService.proposeSchemaFieldTerms(
            mockOpContext, TEST_ENTITY_URN, TEST_FIELD_PATH, requestedTags);
    assertNotNull(result);

    // Check we propose exactly 1 new tag
    ArgumentCaptor<List<MetadataChangeProposal>> batchMcpCaptor =
        ArgumentCaptor.forClass((Class) List.class);
    verify(mockEntityClient, times(1))
        .batchIngestProposals(eq(mockOpContext), batchMcpCaptor.capture(), eq(false));
    List<MetadataChangeProposal> batchMcps = batchMcpCaptor.getValue();

    MetadataChangeProposal infoProposal = batchMcps.get(0);
    ActionRequestInfo info =
        GenericRecordUtils.deserializeAspect(
            infoProposal.getAspect().getValue(),
            infoProposal.getAspect().getContentType(),
            ActionRequestInfo.class);
    assertEquals(info.getParams().getGlossaryTermProposal().getGlossaryTerms().size(), 1);
    assertEquals(
        info.getParams().getGlossaryTermProposal().getGlossaryTerms().get(0), TEST_TERM_URN_2);
  }

  @Test(expectedExceptions = AlreadyAppliedException.class)
  public void testProposeSchemaFieldTermsAllApplied() throws Exception {
    // Both tags already applied on the field
    GlossaryTermAssociation term1 =
        new GlossaryTermAssociation().setUrn(GlossaryTermUrn.createFromUrn(TEST_TERM_URN));
    GlossaryTermAssociation term2 =
        new GlossaryTermAssociation().setUrn(GlossaryTermUrn.createFromUrn(TEST_TERM_URN_2));
    when(mockGlossaryTermService.getSchemaFieldTerms(
            eq(mockOpContext), eq(TEST_ENTITY_URN), eq(TEST_FIELD_PATH)))
        .thenReturn(Arrays.asList(term1, term2));

    actionRequestService.proposeSchemaFieldTerms(
        mockOpContext,
        TEST_ENTITY_URN,
        TEST_FIELD_PATH,
        Arrays.asList(TEST_TERM_URN, TEST_TERM_URN_2));
  }

  @Test(expectedExceptions = AlreadyRequestedException.class)
  public void testProposeSchemaFieldTermsAllRequested() throws Exception {
    // None are actually applied, but both tags are already requested
    when(mockTagService.getSchemaFieldTags(
            eq(mockOpContext), eq(TEST_ENTITY_URN), eq(TEST_FIELD_PATH)))
        .thenReturn(Collections.emptyList());

    // Stub the search to return an existing action request with both tags
    SearchResult mockSearchResult = new SearchResult();
    SearchEntity searchEntity = new SearchEntity();
    Urn existingActionRequestUrn = Urn.createFromString("urn:li:actionRequest:XYZ");
    searchEntity.setEntity(existingActionRequestUrn);
    mockSearchResult.setEntities(new SearchEntityArray(Collections.singletonList(searchEntity)));
    when(mockEntityClient.filter(
            any(OperationContext.class),
            eq(Constants.ACTION_REQUEST_ENTITY_NAME),
            any(Filter.class),
            any(),
            anyInt(),
            anyInt()))
        .thenReturn(mockSearchResult);

    EntityResponse mockEntityResponse = mock(EntityResponse.class);
    Map<Urn, EntityResponse> mockBatchGetMap = new HashMap<>();
    mockBatchGetMap.put(existingActionRequestUrn, mockEntityResponse);

    when(mockEntityClient.batchGetV2(
            eq(mockOpContext), eq(Constants.ACTION_REQUEST_ENTITY_NAME), anySet(), anySet()))
        .thenReturn(mockBatchGetMap);

    ActionRequestInfo existingActionRequestInfo = new ActionRequestInfo();
    GlossaryTermProposal termProposal = new GlossaryTermProposal();
    termProposal.setGlossaryTerms(new UrnArray(Arrays.asList(TEST_TERM_URN, TEST_TERM_URN_2)));
    ActionRequestParams params = new ActionRequestParams().setGlossaryTermProposal(termProposal);
    existingActionRequestInfo.setParams(params);
    existingActionRequestInfo.setSubResource(TEST_FIELD_PATH);
    existingActionRequestInfo.setSubResourceType(SubResourceType.DATASET_FIELD.toString());

    ActionRequestStatus existingActionRequestStatus =
        new ActionRequestStatus().setStatus(ACTION_REQUEST_STATUS_PENDING);

    when(mockEntityResponse.hasAspects()).thenReturn(true);
    when(mockEntityResponse.getAspects())
        .thenReturn(
            new EnvelopedAspectMap(
                ActionRequestTestUtils.mockAspectsMapForActionRequest(
                    existingActionRequestInfo, existingActionRequestStatus)));

    // Now propose both tags
    actionRequestService.proposeSchemaFieldTerms(
        mockOpContext,
        TEST_ENTITY_URN,
        TEST_FIELD_PATH,
        Arrays.asList(TEST_TERM_URN, TEST_TERM_URN_2));
  }

  @Test(expectedExceptions = MalformedActionRequestException.class)
  public void testProposeSchemaFieldTermsEntityNotDataset() throws Exception {
    // Given
    Urn someOtherEntityUrn = Urn.createFromString("urn:li:chart:123");

    // When
    actionRequestService.proposeSchemaFieldTerms(
        mockOpContext,
        someOtherEntityUrn,
        TEST_FIELD_PATH,
        Collections.singletonList(TEST_TERM_URN));
  }

  // ============================================================================
  // TESTS FOR proposeEntityStructuredProperties(...)
  // ============================================================================

  @Test
  public void testProposeEntityStructuredPropertiesSuccess() throws Exception {
    // Given
    when(mockStructuredPropertyService.getEntityStructuredProperties(
            eq(mockOpContext), eq(TEST_ENTITY_URN)))
        .thenReturn(Collections.emptyList()); // No properties currently applied

    StructuredPropertyValueAssignment structuredPropertyAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(PrimitivePropertyValue.create("testValue"))));

    // When
    Urn result =
        actionRequestService.proposeEntityStructuredProperties(
            mockOpContext,
            TEST_ENTITY_URN,
            Collections.singletonList(structuredPropertyAssignment));

    // Then
    assertNotNull(
        result, "Returned URN from proposeEntityStructuredProperties should not be null.");

    // 1) Capture the proposals passed to batchIngestProposals
    ArgumentCaptor<List<MetadataChangeProposal>> batchMcpCaptor =
        ArgumentCaptor.forClass((Class) List.class);
    verify(mockEntityClient, times(1))
        .batchIngestProposals(eq(mockOpContext), batchMcpCaptor.capture(), eq(false));
    List<MetadataChangeProposal> batchMcps = batchMcpCaptor.getValue();

    assertEquals(batchMcps.size(), 2, "Expect 2 MCPs for ActionRequestInfo + ActionRequestStatus");

    // Validate aspects
    MetadataChangeProposal infoProposal = batchMcps.get(0);
    MetadataChangeProposal statusProposal = batchMcps.get(1);

    assertEquals(infoProposal.getAspectName(), Constants.ACTION_REQUEST_INFO_ASPECT_NAME);
    assertEquals(statusProposal.getAspectName(), Constants.ACTION_REQUEST_STATUS_ASPECT_NAME);

    // Check that the entityUrn is an actionRequest URN
    assertEquals(infoProposal.getEntityUrn().getEntityType(), Constants.ACTION_REQUEST_ENTITY_NAME);

    // Deserialize & check ActionRequestInfo
    ActionRequestInfo info =
        GenericRecordUtils.deserializeAspect(
            infoProposal.getAspect().getValue(),
            infoProposal.getAspect().getContentType(),
            ActionRequestInfo.class);
    assertEquals(info.getType(), STRUCTURED_PROPERTY_ASSOCIATION_PROPOSAL_TYPE);
    assertTrue(info.getParams().getStructuredPropertyProposal().hasStructuredPropertyValues());
    assertEquals(
        info.getParams().getStructuredPropertyProposal().getStructuredPropertyValues().size(), 1);
    assertEquals(
        info.getParams()
            .getStructuredPropertyProposal()
            .getStructuredPropertyValues()
            .get(0)
            .getPropertyUrn(),
        TEST_STRUCTURED_PROPERTY_URN_1);

    // Deserialize & check ActionRequestStatus
    ActionRequestStatus status =
        GenericRecordUtils.deserializeAspect(
            statusProposal.getAspect().getValue(),
            statusProposal.getAspect().getContentType(),
            ActionRequestStatus.class);
    assertEquals(status.getStatus(), ACTION_REQUEST_STATUS_PENDING);
  }

  @Test(expectedExceptions = EntityDoesNotExistException.class)
  public void testProposeEntityStructuredPropertiesEntityDoesNotExist() throws Exception {
    // Given
    when(mockEntityClient.exists(eq(mockOpContext), eq(TEST_ENTITY_URN), eq(false)))
        .thenReturn(false);

    StructuredPropertyValueAssignment structuredPropertyAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(PrimitivePropertyValue.create("testValue"))));

    // When
    actionRequestService.proposeEntityStructuredProperties(
        mockOpContext, TEST_ENTITY_URN, Collections.singletonList(structuredPropertyAssignment));
  }

  @Test(expectedExceptions = EntityDoesNotExistException.class)
  public void testProposeEntityStructuredPropertiesPropertyDoesNotExist() throws Exception {
    // Given
    when(mockEntityClient.exists(eq(mockOpContext), eq(TEST_STRUCTURED_PROPERTY_URN_1), eq(false)))
        .thenReturn(false);

    StructuredPropertyValueAssignment structuredPropertyAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(PrimitivePropertyValue.create("testValue"))));

    // When
    actionRequestService.proposeEntityStructuredProperties(
        mockOpContext, TEST_ENTITY_URN, Collections.singletonList(structuredPropertyAssignment));
  }

  // =======================
  // Filtering-Focused Tests
  // =======================
  @Test
  public void testProposeEntityStructuredPropertiesSomeAlreadyApplied() throws Exception {
    // One property already applied, the other is new
    StructuredPropertyValueAssignment alreadyAppliedProperty =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(PrimitivePropertyValue.create("testValue"))));

    when(mockStructuredPropertyService.getEntityStructuredProperties(
            eq(mockOpContext), eq(TEST_ENTITY_URN)))
        .thenReturn(Collections.singletonList(alreadyAppliedProperty));

    StructuredPropertyValueAssignment nonExistantStructuredPropertyProposed =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_2)
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(PrimitivePropertyValue.create("testValue2"))));

    // Expect success, but only the new property gets added
    Urn result =
        actionRequestService.proposeEntityStructuredProperties(
            mockOpContext,
            TEST_ENTITY_URN,
            ImmutableList.of(alreadyAppliedProperty, nonExistantStructuredPropertyProposed));
    assertNotNull(result);

    // Capture the batchIngestProposals
    ArgumentCaptor<List<MetadataChangeProposal>> batchMcpCaptor =
        ArgumentCaptor.forClass((Class) List.class);
    verify(mockEntityClient, times(1))
        .batchIngestProposals(eq(mockOpContext), batchMcpCaptor.capture(), eq(false));
    List<MetadataChangeProposal> batchMcps = batchMcpCaptor.getValue();

    // The first MCP should be the ActionRequestInfo
    MetadataChangeProposal infoProposal = batchMcps.get(0);
    ActionRequestInfo info =
        GenericRecordUtils.deserializeAspect(
            infoProposal.getAspect().getValue(),
            infoProposal.getAspect().getContentType(),
            ActionRequestInfo.class);

    List<StructuredPropertyValueAssignment> propertiesInProposal =
        info.getParams().getStructuredPropertyProposal().getStructuredPropertyValues();
    assertEquals(propertiesInProposal.size(), 1, "Should only propose the new property value");
    assertEquals(propertiesInProposal.get(0).getPropertyUrn(), TEST_STRUCTURED_PROPERTY_URN_2);
    assertEquals(
        propertiesInProposal.get(0).getValues(), nonExistantStructuredPropertyProposed.getValues());
  }

  @Test(expectedExceptions = AlreadyAppliedException.class)
  public void testProposeEntityStructuredPropertiesAllApplied() throws Exception {
    StructuredPropertyValueAssignment alreadyAppliedProperty =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(PrimitivePropertyValue.create("testValue"))));

    when(mockStructuredPropertyService.getEntityStructuredProperties(
            eq(mockOpContext), eq(TEST_ENTITY_URN)))
        .thenReturn(Collections.singletonList(alreadyAppliedProperty));

    // No previously proposed tags
    actionRequestService.proposeEntityStructuredProperties(
        mockOpContext, TEST_ENTITY_URN, Collections.singletonList(alreadyAppliedProperty));
  }

  @Test(expectedExceptions = AlreadyRequestedException.class)
  public void testProposeEntityStructuredPropertiesAllRequested() throws Exception {
    // Suppose no tags are actually applied, but both properties are already proposed
    when(mockStructuredPropertyService.getEntityStructuredProperties(
            eq(mockOpContext), eq(TEST_ENTITY_URN)))
        .thenReturn(Collections.emptyList());

    // Stub the search to return an action request with both tags
    SearchResult mockSearchResult = new SearchResult();
    SearchEntity searchEntity = new SearchEntity();
    Urn existingActionRequestUrn = Urn.createFromString("urn:li:actionRequest:123");
    searchEntity.setEntity(existingActionRequestUrn);
    mockSearchResult.setEntities(new SearchEntityArray(Collections.singletonList(searchEntity)));
    when(mockEntityClient.filter(
            any(OperationContext.class),
            eq(Constants.ACTION_REQUEST_ENTITY_NAME),
            any(Filter.class),
            any(),
            anyInt(),
            anyInt()))
        .thenReturn(mockSearchResult);

    EntityResponse mockEntityResponse = mock(EntityResponse.class);
    Map<Urn, EntityResponse> mockBatchGetMap = new HashMap<>();
    mockBatchGetMap.put(existingActionRequestUrn, mockEntityResponse);

    when(mockEntityClient.batchGetV2(
            eq(mockOpContext), eq(Constants.ACTION_REQUEST_ENTITY_NAME), anySet(), anySet()))
        .thenReturn(mockBatchGetMap);

    // The existing action request has both tags
    ActionRequestInfo existingActionRequestInfo = new ActionRequestInfo();
    StructuredPropertyProposal structuredPropertyProposal = new StructuredPropertyProposal();
    structuredPropertyProposal.setStructuredPropertyValues(
        new StructuredPropertyValueAssignmentArray(
            ImmutableList.of(
                new StructuredPropertyValueAssignment()
                    .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
                    .setValues(
                        new PrimitivePropertyValueArray(
                            ImmutableList.of(PrimitivePropertyValue.create("testValue")))))));
    ActionRequestParams params =
        new ActionRequestParams().setStructuredPropertyProposal(structuredPropertyProposal);
    existingActionRequestInfo.setParams(params);

    ActionRequestStatus existingActionRequestStatus =
        new ActionRequestStatus().setStatus(ACTION_REQUEST_STATUS_PENDING);

    when(mockEntityResponse.hasAspects()).thenReturn(true);
    when(mockEntityResponse.getAspects())
        .thenReturn(
            new EnvelopedAspectMap(
                ActionRequestTestUtils.mockAspectsMapForActionRequest(
                    existingActionRequestInfo, existingActionRequestStatus)));

    StructuredPropertyValueAssignment proposedProperty =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(PrimitivePropertyValue.create("testValue"))));

    actionRequestService.proposeEntityStructuredProperties(
        mockOpContext, TEST_ENTITY_URN, Collections.singletonList(proposedProperty));
  }

  @Test(expectedExceptions = MalformedActionRequestException.class)
  public void testProposeEntityStructuredPropertiesValuesAreInvalid() throws Exception {
    // Given
    when(mockStructuredPropertyService.getEntityStructuredProperties(
            eq(mockOpContext), eq(TEST_ENTITY_URN)))
        .thenReturn(Collections.emptyList()); // No properties currently applied

    when(mockStructuredPropertyService.areProposedStructuredPropertyValuesValid(
            eq(mockOpContext), eq(TEST_STRUCTURED_PROPERTY_URN_1), any()))
        .thenReturn(false);

    StructuredPropertyValueAssignment structuredPropertyAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(PrimitivePropertyValue.create("testValue"))));

    // When
    actionRequestService.proposeEntityStructuredProperties(
        mockOpContext, TEST_ENTITY_URN, Collections.singletonList(structuredPropertyAssignment));
  }

  @Test(expectedExceptions = MalformedActionRequestException.class)
  public void testProposeEntityStructuredPropertiesDuplicateProperties() throws Exception {
    // Given
    when(mockStructuredPropertyService.getEntityStructuredProperties(
            eq(mockOpContext), eq(TEST_ENTITY_URN)))
        .thenReturn(Collections.emptyList()); // No properties currently applied

    StructuredPropertyValueAssignment structuredPropertyAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(PrimitivePropertyValue.create("testValue"))));

    StructuredPropertyValueAssignment duplicateStructuredPropertyAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(PrimitivePropertyValue.create("testValue2"))));

    // When
    actionRequestService.proposeEntityStructuredProperties(
        mockOpContext,
        TEST_ENTITY_URN,
        ImmutableList.of(structuredPropertyAssignment, duplicateStructuredPropertyAssignment));
  }

  @Test
  public void testProposeExistingEntityPropertyDifferentValues() throws Exception {
    // You can propose a property that is already applied, or already proposed, with a different
    // VALUE for the proposed values.

    StructuredPropertyValueAssignment alreadyAppliedProperty =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(PrimitivePropertyValue.create("testValueExisting"))));

    // Suppose no tags are actually applied, but both properties are already proposed
    when(mockStructuredPropertyService.getEntityStructuredProperties(
            eq(mockOpContext), eq(TEST_ENTITY_URN)))
        .thenReturn(Collections.singletonList(alreadyAppliedProperty));

    // Stub the search to return an action request with both tags
    SearchResult mockSearchResult = new SearchResult();
    SearchEntity searchEntity = new SearchEntity();
    Urn existingActionRequestUrn = Urn.createFromString("urn:li:actionRequest:123");
    searchEntity.setEntity(existingActionRequestUrn);
    mockSearchResult.setEntities(new SearchEntityArray(Collections.singletonList(searchEntity)));
    when(mockEntityClient.filter(
            any(OperationContext.class),
            eq(Constants.ACTION_REQUEST_ENTITY_NAME),
            any(Filter.class),
            any(),
            anyInt(),
            anyInt()))
        .thenReturn(mockSearchResult);

    EntityResponse mockEntityResponse = mock(EntityResponse.class);
    Map<Urn, EntityResponse> mockBatchGetMap = new HashMap<>();
    mockBatchGetMap.put(existingActionRequestUrn, mockEntityResponse);

    when(mockEntityClient.batchGetV2(
            eq(mockOpContext), eq(Constants.ACTION_REQUEST_ENTITY_NAME), anySet(), anySet()))
        .thenReturn(mockBatchGetMap);

    // The existing action request has both tags
    ActionRequestInfo existingActionRequestInfo = new ActionRequestInfo();
    StructuredPropertyProposal structuredPropertyProposal = new StructuredPropertyProposal();
    structuredPropertyProposal.setStructuredPropertyValues(
        new StructuredPropertyValueAssignmentArray(
            ImmutableList.of(
                new StructuredPropertyValueAssignment()
                    .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
                    .setValues(
                        new PrimitivePropertyValueArray(
                            ImmutableList.of(
                                PrimitivePropertyValue.create("testValueProposed")))))));
    ActionRequestParams params =
        new ActionRequestParams().setStructuredPropertyProposal(structuredPropertyProposal);
    existingActionRequestInfo.setParams(params);

    ActionRequestStatus existingActionRequestStatus =
        new ActionRequestStatus().setStatus(ACTION_REQUEST_STATUS_PENDING);

    when(mockEntityResponse.hasAspects()).thenReturn(true);
    when(mockEntityResponse.getAspects())
        .thenReturn(
            new EnvelopedAspectMap(
                ActionRequestTestUtils.mockAspectsMapForActionRequest(
                    existingActionRequestInfo, existingActionRequestStatus)));

    StructuredPropertyValueAssignment proposedProperty =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(
                        PrimitivePropertyValue.create("testValueThatDoesNotExistYet"))));

    // Expect success, but only the new property gets added
    Urn result =
        actionRequestService.proposeEntityStructuredProperties(
            mockOpContext, TEST_ENTITY_URN, ImmutableList.of(proposedProperty));
    assertNotNull(result);

    // Capture the batchIngestProposals
    ArgumentCaptor<List<MetadataChangeProposal>> batchMcpCaptor =
        ArgumentCaptor.forClass((Class) List.class);
    verify(mockEntityClient, times(1))
        .batchIngestProposals(eq(mockOpContext), batchMcpCaptor.capture(), eq(false));
    List<MetadataChangeProposal> batchMcps = batchMcpCaptor.getValue();

    // The first MCP should be the ActionRequestInfo
    MetadataChangeProposal infoProposal = batchMcps.get(0);
    ActionRequestInfo info =
        GenericRecordUtils.deserializeAspect(
            infoProposal.getAspect().getValue(),
            infoProposal.getAspect().getContentType(),
            ActionRequestInfo.class);
    List<StructuredPropertyValueAssignment> propertiesInProposal =
        info.getParams().getStructuredPropertyProposal().getStructuredPropertyValues();
    assertEquals(propertiesInProposal.size(), 1, "Should only propose the new property value");
    assertEquals(propertiesInProposal.get(0).getPropertyUrn(), TEST_STRUCTURED_PROPERTY_URN_1);
    assertEquals(propertiesInProposal.get(0).getValues(), proposedProperty.getValues());
  }

  // ============================================================================
  // TESTS FOR proposeSchemaFieldStructuredProperties(...)
  // ============================================================================
  @Test
  public void testProposeSchemaFieldStructuredPropertiesSuccess() throws Exception {
    // Given
    when(mockStructuredPropertyService.getSchemaFieldStructuredProperties(
            eq(mockOpContext), eq(TEST_ENTITY_URN), eq(TEST_FIELD_PATH)))
        .thenReturn(Collections.emptyList()); // No properties currently applied

    StructuredPropertyValueAssignment structuredPropertyAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(PrimitivePropertyValue.create("testValue"))));

    // When
    Urn result =
        actionRequestService.proposeSchemaFieldStructuredProperties(
            mockOpContext,
            TEST_ENTITY_URN,
            TEST_FIELD_PATH,
            Collections.singletonList(structuredPropertyAssignment));

    // Then
    assertNotNull(
        result, "Returned URN from proposeSchemaFieldStructuredProperties should not be null.");

    // 1) Capture the proposals passed to batchIngestProposals
    ArgumentCaptor<List<MetadataChangeProposal>> batchMcpCaptor =
        ArgumentCaptor.forClass((Class) List.class);
    verify(mockEntityClient, times(1))
        .batchIngestProposals(eq(mockOpContext), batchMcpCaptor.capture(), eq(false));
    List<MetadataChangeProposal> batchMcps = batchMcpCaptor.getValue();

    assertEquals(batchMcps.size(), 2, "Expect 2 MCPs for ActionRequestInfo + ActionRequestStatus");

    // Validate aspects
    MetadataChangeProposal infoProposal = batchMcps.get(0);
    MetadataChangeProposal statusProposal = batchMcps.get(1);

    assertEquals(infoProposal.getAspectName(), Constants.ACTION_REQUEST_INFO_ASPECT_NAME);
    assertEquals(statusProposal.getAspectName(), Constants.ACTION_REQUEST_STATUS_ASPECT_NAME);

    // Check that the entityUrn is an actionRequest URN
    assertEquals(infoProposal.getEntityUrn().getEntityType(), Constants.ACTION_REQUEST_ENTITY_NAME);

    // Deserialize & check ActionRequestInfo
    ActionRequestInfo info =
        GenericRecordUtils.deserializeAspect(
            infoProposal.getAspect().getValue(),
            infoProposal.getAspect().getContentType(),
            ActionRequestInfo.class);
    assertEquals(info.getType(), STRUCTURED_PROPERTY_ASSOCIATION_PROPOSAL_TYPE);
    assertTrue(info.getParams().getStructuredPropertyProposal().hasStructuredPropertyValues());
    assertEquals(info.getSubResource(), TEST_FIELD_PATH);
    assertEquals(info.getSubResourceType(), SubResourceType.DATASET_FIELD.toString());
    assertEquals(
        info.getParams().getStructuredPropertyProposal().getStructuredPropertyValues().size(), 1);
    assertEquals(
        info.getParams()
            .getStructuredPropertyProposal()
            .getStructuredPropertyValues()
            .get(0)
            .getPropertyUrn(),
        TEST_STRUCTURED_PROPERTY_URN_1);

    // Deserialize & check ActionRequestStatus
    ActionRequestStatus status =
        GenericRecordUtils.deserializeAspect(
            statusProposal.getAspect().getValue(),
            statusProposal.getAspect().getContentType(),
            ActionRequestStatus.class);
    assertEquals(status.getStatus(), ACTION_REQUEST_STATUS_PENDING);
  }

  @Test(expectedExceptions = EntityDoesNotExistException.class)
  public void testProposeSchemaFieldStructuredPropertiesEntityDoesNotExist() throws Exception {
    // Given
    when(mockEntityClient.exists(eq(mockOpContext), eq(TEST_ENTITY_URN), eq(false)))
        .thenReturn(false);

    StructuredPropertyValueAssignment structuredPropertyAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(PrimitivePropertyValue.create("testValue"))));

    // When
    actionRequestService.proposeSchemaFieldStructuredProperties(
        mockOpContext,
        TEST_ENTITY_URN,
        TEST_FIELD_PATH,
        Collections.singletonList(structuredPropertyAssignment));
  }

  @Test(expectedExceptions = EntityDoesNotExistException.class)
  public void testProposeSchemaFieldStructuredPropertiesPropertyDoesNotExist() throws Exception {
    // Given
    when(mockEntityClient.exists(eq(mockOpContext), eq(TEST_STRUCTURED_PROPERTY_URN_1), eq(false)))
        .thenReturn(false);

    StructuredPropertyValueAssignment structuredPropertyAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(PrimitivePropertyValue.create("testValue"))));

    // When
    actionRequestService.proposeSchemaFieldStructuredProperties(
        mockOpContext,
        TEST_ENTITY_URN,
        TEST_FIELD_PATH,
        Collections.singletonList(structuredPropertyAssignment));
  }

  // =======================
  // Filtering-Focused Tests
  // =======================
  @Test
  public void testProposeSchemaFieldStructuredPropertiesSomeAlreadyApplied() throws Exception {
    // One property already applied, the other is new
    StructuredPropertyValueAssignment alreadyAppliedProperty =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(PrimitivePropertyValue.create("testValue"))));

    when(mockStructuredPropertyService.getSchemaFieldStructuredProperties(
            eq(mockOpContext), eq(TEST_ENTITY_URN), eq(TEST_FIELD_PATH)))
        .thenReturn(Collections.singletonList(alreadyAppliedProperty));

    StructuredPropertyValueAssignment nonExistantStructuredPropertyProposed =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_2)
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(PrimitivePropertyValue.create("testValue2"))));

    // Expect success, but only the new property gets added
    Urn result =
        actionRequestService.proposeSchemaFieldStructuredProperties(
            mockOpContext,
            TEST_ENTITY_URN,
            TEST_FIELD_PATH,
            ImmutableList.of(alreadyAppliedProperty, nonExistantStructuredPropertyProposed));
    assertNotNull(result);

    // Capture the batchIngestProposals
    ArgumentCaptor<List<MetadataChangeProposal>> batchMcpCaptor =
        ArgumentCaptor.forClass((Class) List.class);
    verify(mockEntityClient, times(1))
        .batchIngestProposals(eq(mockOpContext), batchMcpCaptor.capture(), eq(false));
    List<MetadataChangeProposal> batchMcps = batchMcpCaptor.getValue();

    // The first MCP should be the ActionRequestInfo
    MetadataChangeProposal infoProposal = batchMcps.get(0);
    ActionRequestInfo info =
        GenericRecordUtils.deserializeAspect(
            infoProposal.getAspect().getValue(),
            infoProposal.getAspect().getContentType(),
            ActionRequestInfo.class);

    List<StructuredPropertyValueAssignment> propertiesInProposal =
        info.getParams().getStructuredPropertyProposal().getStructuredPropertyValues();
    assertEquals(propertiesInProposal.size(), 1, "Should only propose the new property value");
    assertEquals(propertiesInProposal.get(0).getPropertyUrn(), TEST_STRUCTURED_PROPERTY_URN_2);
    assertEquals(
        propertiesInProposal.get(0).getValues(), nonExistantStructuredPropertyProposed.getValues());
  }

  @Test(expectedExceptions = AlreadyAppliedException.class)
  public void testProposeSchemaFieldStructuredPropertiesAllApplied() throws Exception {
    StructuredPropertyValueAssignment alreadyAppliedProperty =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(PrimitivePropertyValue.create("testValue"))));

    when(mockStructuredPropertyService.getSchemaFieldStructuredProperties(
            eq(mockOpContext), eq(TEST_ENTITY_URN), eq(TEST_FIELD_PATH)))
        .thenReturn(Collections.singletonList(alreadyAppliedProperty));

    // No previously proposed structured properties
    actionRequestService.proposeSchemaFieldStructuredProperties(
        mockOpContext,
        TEST_ENTITY_URN,
        TEST_FIELD_PATH,
        Collections.singletonList(alreadyAppliedProperty));
  }

  @Test(expectedExceptions = AlreadyRequestedException.class)
  public void testProposeSchemaFieldStructuredPropertiesAllRequested() throws Exception {
    // Suppose no tags are actually applied, but both properties are already proposed
    when(mockStructuredPropertyService.getSchemaFieldStructuredProperties(
            eq(mockOpContext), eq(TEST_ENTITY_URN), eq(TEST_FIELD_PATH)))
        .thenReturn(Collections.emptyList());

    // Stub the search to return an action request with both tags
    SearchResult mockSearchResult = new SearchResult();
    SearchEntity searchEntity = new SearchEntity();
    Urn existingActionRequestUrn = Urn.createFromString("urn:li:actionRequest:123");
    searchEntity.setEntity(existingActionRequestUrn);
    mockSearchResult.setEntities(new SearchEntityArray(Collections.singletonList(searchEntity)));
    when(mockEntityClient.filter(
            any(OperationContext.class),
            eq(Constants.ACTION_REQUEST_ENTITY_NAME),
            any(Filter.class),
            any(),
            anyInt(),
            anyInt()))
        .thenReturn(mockSearchResult);

    EntityResponse mockEntityResponse = mock(EntityResponse.class);
    Map<Urn, EntityResponse> mockBatchGetMap = new HashMap<>();
    mockBatchGetMap.put(existingActionRequestUrn, mockEntityResponse);

    when(mockEntityClient.batchGetV2(
            eq(mockOpContext), eq(Constants.ACTION_REQUEST_ENTITY_NAME), anySet(), anySet()))
        .thenReturn(mockBatchGetMap);

    // The existing action request has both tags
    ActionRequestInfo existingActionRequestInfo = new ActionRequestInfo();
    StructuredPropertyProposal structuredPropertyProposal = new StructuredPropertyProposal();
    structuredPropertyProposal.setStructuredPropertyValues(
        new StructuredPropertyValueAssignmentArray(
            ImmutableList.of(
                new StructuredPropertyValueAssignment()
                    .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
                    .setValues(
                        new PrimitivePropertyValueArray(
                            ImmutableList.of(PrimitivePropertyValue.create("testValue")))))));
    ActionRequestParams params =
        new ActionRequestParams().setStructuredPropertyProposal(structuredPropertyProposal);
    existingActionRequestInfo.setParams(params);
    existingActionRequestInfo.setSubResourceType(SubResourceType.DATASET_FIELD.toString());
    existingActionRequestInfo.setSubResource(TEST_FIELD_PATH);

    ActionRequestStatus existingActionRequestStatus =
        new ActionRequestStatus().setStatus(ACTION_REQUEST_STATUS_PENDING);

    when(mockEntityResponse.hasAspects()).thenReturn(true);
    when(mockEntityResponse.getAspects())
        .thenReturn(
            new EnvelopedAspectMap(
                ActionRequestTestUtils.mockAspectsMapForActionRequest(
                    existingActionRequestInfo, existingActionRequestStatus)));

    StructuredPropertyValueAssignment proposedProperty =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(PrimitivePropertyValue.create("testValue"))));

    actionRequestService.proposeSchemaFieldStructuredProperties(
        mockOpContext,
        TEST_ENTITY_URN,
        TEST_FIELD_PATH,
        Collections.singletonList(proposedProperty));
  }

  @Test(expectedExceptions = MalformedActionRequestException.class)
  public void testProposeSchemaFieldStructuredPropertiesValuesAreInvalid() throws Exception {
    // Given
    when(mockStructuredPropertyService.getSchemaFieldStructuredProperties(
            eq(mockOpContext), eq(TEST_ENTITY_URN), eq(TEST_FIELD_PATH)))
        .thenReturn(Collections.emptyList()); // No properties currently applied

    when(mockStructuredPropertyService.areProposedStructuredPropertyValuesValid(
            eq(mockOpContext), eq(TEST_STRUCTURED_PROPERTY_URN_1), any()))
        .thenReturn(false);

    StructuredPropertyValueAssignment structuredPropertyAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(PrimitivePropertyValue.create("testValue"))));

    // When
    actionRequestService.proposeSchemaFieldStructuredProperties(
        mockOpContext,
        TEST_ENTITY_URN,
        TEST_FIELD_PATH,
        Collections.singletonList(structuredPropertyAssignment));
  }

  @Test(expectedExceptions = MalformedActionRequestException.class)
  public void testProposeSchemaFieldStructuredPropertiesDuplicateProperties() throws Exception {
    // Given
    when(mockStructuredPropertyService.getSchemaFieldStructuredProperties(
            eq(mockOpContext), eq(TEST_ENTITY_URN), eq(TEST_FIELD_PATH)))
        .thenReturn(Collections.emptyList()); // No properties currently applied

    StructuredPropertyValueAssignment structuredPropertyAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(PrimitivePropertyValue.create("testValue"))));

    StructuredPropertyValueAssignment duplicateStructuredPropertyAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(PrimitivePropertyValue.create("testValue2"))));

    // When
    actionRequestService.proposeSchemaFieldStructuredProperties(
        mockOpContext,
        TEST_ENTITY_URN,
        TEST_FIELD_PATH,
        ImmutableList.of(structuredPropertyAssignment, duplicateStructuredPropertyAssignment));
  }

  @Test
  public void testProposeExistingSchemaFieldPropertyDifferentValues() throws Exception {
    // You can propose a property that is already applied, or already proposed, with a different
    // VALUE for the proposed values.

    StructuredPropertyValueAssignment alreadyAppliedProperty =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(PrimitivePropertyValue.create("testValueExisting"))));

    // Suppose no tags are actually applied, but both properties are already proposed
    when(mockStructuredPropertyService.getSchemaFieldStructuredProperties(
            eq(mockOpContext), eq(TEST_ENTITY_URN), eq(TEST_FIELD_PATH)))
        .thenReturn(Collections.singletonList(alreadyAppliedProperty));

    // Stub the search to return an action request with both tags
    SearchResult mockSearchResult = new SearchResult();
    SearchEntity searchEntity = new SearchEntity();
    Urn existingActionRequestUrn = Urn.createFromString("urn:li:actionRequest:123");
    searchEntity.setEntity(existingActionRequestUrn);
    mockSearchResult.setEntities(new SearchEntityArray(Collections.singletonList(searchEntity)));
    when(mockEntityClient.filter(
            any(OperationContext.class),
            eq(Constants.ACTION_REQUEST_ENTITY_NAME),
            any(Filter.class),
            any(),
            anyInt(),
            anyInt()))
        .thenReturn(mockSearchResult);

    EntityResponse mockEntityResponse = mock(EntityResponse.class);
    Map<Urn, EntityResponse> mockBatchGetMap = new HashMap<>();
    mockBatchGetMap.put(existingActionRequestUrn, mockEntityResponse);

    when(mockEntityClient.batchGetV2(
            eq(mockOpContext), eq(Constants.ACTION_REQUEST_ENTITY_NAME), anySet(), anySet()))
        .thenReturn(mockBatchGetMap);

    // The existing action request has both tags
    ActionRequestInfo existingActionRequestInfo = new ActionRequestInfo();
    StructuredPropertyProposal structuredPropertyProposal = new StructuredPropertyProposal();
    structuredPropertyProposal.setStructuredPropertyValues(
        new StructuredPropertyValueAssignmentArray(
            ImmutableList.of(
                new StructuredPropertyValueAssignment()
                    .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
                    .setValues(
                        new PrimitivePropertyValueArray(
                            ImmutableList.of(
                                PrimitivePropertyValue.create("testValueProposed")))))));
    ActionRequestParams params =
        new ActionRequestParams().setStructuredPropertyProposal(structuredPropertyProposal);
    existingActionRequestInfo.setParams(params);

    ActionRequestStatus existingActionRequestStatus =
        new ActionRequestStatus().setStatus(ACTION_REQUEST_STATUS_PENDING);

    when(mockEntityResponse.hasAspects()).thenReturn(true);
    when(mockEntityResponse.getAspects())
        .thenReturn(
            new EnvelopedAspectMap(
                ActionRequestTestUtils.mockAspectsMapForActionRequest(
                    existingActionRequestInfo, existingActionRequestStatus)));

    StructuredPropertyValueAssignment proposedProperty =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(
                        PrimitivePropertyValue.create("testValueThatDoesNotExistYet"))));

    // Expect success, but only the new property gets added
    Urn result =
        actionRequestService.proposeSchemaFieldStructuredProperties(
            mockOpContext, TEST_ENTITY_URN, TEST_FIELD_PATH, ImmutableList.of(proposedProperty));
    assertNotNull(result);

    // Capture the batchIngestProposals
    ArgumentCaptor<List<MetadataChangeProposal>> batchMcpCaptor =
        ArgumentCaptor.forClass((Class) List.class);
    verify(mockEntityClient, times(1))
        .batchIngestProposals(eq(mockOpContext), batchMcpCaptor.capture(), eq(false));
    List<MetadataChangeProposal> batchMcps = batchMcpCaptor.getValue();

    // The first MCP should be the ActionRequestInfo
    MetadataChangeProposal infoProposal = batchMcps.get(0);
    ActionRequestInfo info =
        GenericRecordUtils.deserializeAspect(
            infoProposal.getAspect().getValue(),
            infoProposal.getAspect().getContentType(),
            ActionRequestInfo.class);
    List<StructuredPropertyValueAssignment> propertiesInProposal =
        info.getParams().getStructuredPropertyProposal().getStructuredPropertyValues();
    assertEquals(propertiesInProposal.size(), 1, "Should only propose the new property value");
    assertEquals(propertiesInProposal.get(0).getPropertyUrn(), TEST_STRUCTURED_PROPERTY_URN_1);
    assertEquals(propertiesInProposal.get(0).getValues(), proposedProperty.getValues());
  }

  // --------------------------------------------------------------------------------
  // Tests for acceptStructuredPropertyProposal(OperationContext, Urn)
  // --------------------------------------------------------------------------------

  @Test(
      expectedExceptions = EntityDoesNotExistException.class,
      expectedExceptionsMessageRegExp = "Action request with urn .* does not exist.")
  public void
      testAcceptStructuredPropertyProposalByUrnActionRequestNotFoundThrowsEntityDoesNotExist()
          throws Exception {
    // Given
    Urn actionRequestUrn = Urn.createFromString("urn:li:actionRequest:123");

    // getActionRequestInfo returns null -> simulating not found
    doReturn(null)
        .when(mockEntityClient)
        .getV2(
            any(OperationContext.class),
            Mockito.eq(Constants.ACTION_REQUEST_ENTITY_NAME),
            Mockito.eq(actionRequestUrn),
            any());

    // When
    actionRequestService.acceptStructuredPropertyProposal(mockOpContext, actionRequestUrn);

    // Then -> Expect EntityDoesNotExistException
  }

  @Test
  public void testAcceptStructuredPropertyProposalSuccess() throws Exception {
    AuditStamp auditStamp = new AuditStamp().setTime(1234L).setActor(TEST_ACTOR_URN);
    Mockito.when(mockOpContext.getAuditStamp()).thenReturn(auditStamp);

    // Given
    Urn actionRequestUrn = Urn.createFromString("urn:li:actionRequest:123");

    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType(ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL);
    actionRequestInfo.setResource(TEST_ENTITY_URN.toString());
    actionRequestInfo.setParams(
        new ActionRequestParams()
            .setStructuredPropertyProposal(
                new StructuredPropertyProposal()
                    .setStructuredPropertyValues(
                        new StructuredPropertyValueAssignmentArray(
                            ImmutableList.of(
                                new StructuredPropertyValueAssignment()
                                    .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
                                    .setValues(
                                        new PrimitivePropertyValueArray(
                                            ImmutableList.of(
                                                PrimitivePropertyValue.create("testValue")))))))));

    EntityResponse response = new EntityResponse();
    response.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                Constants.ACTION_REQUEST_INFO_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(actionRequestInfo.data())))));

    // Set up the ActionRequestInfo as needed for the next method call
    doReturn(response)
        .when(mockEntityClient)
        .getV2(
            any(OperationContext.class),
            Mockito.eq(Constants.ACTION_REQUEST_ENTITY_NAME),
            Mockito.eq(actionRequestUrn),
            any());

    // When
    actionRequestService.acceptStructuredPropertyProposal(mockOpContext, actionRequestUrn);

    // Just ensure that the mock structured property service gets called
    verify(mockStructuredPropertyService, times(1))
        .updateEntityStructuredProperties(
            eq(mockOpContext),
            eq(TEST_ENTITY_URN),
            eq(
                actionRequestInfo
                    .getParams()
                    .getStructuredPropertyProposal()
                    .getStructuredPropertyValues()));
  }

  // --------------------------------------------------------------------------------
  // Tests for acceptStructuredPropertyProposal(OperationContext, ActionRequestInfo)
  // --------------------------------------------------------------------------------

  @Test(
      expectedExceptions = MalformedActionRequestException.class,
      expectedExceptionsMessageRegExp = "Action request is not a structured property proposal")
  public void testAcceptStructuredPropertyProposalByInfo_WrongType_ThrowsMalformedActionRequest()
      throws Exception {
    // Given
    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType("SOME_OTHER_TYPE"); // not the structured proposal type

    // When
    actionRequestService.acceptStructuredPropertyProposal(mockOpContext, actionRequestInfo);

    // Then -> Expect MalformedActionRequestException
  }

  @Test(
      expectedExceptions = MalformedActionRequestException.class,
      expectedExceptionsMessageRegExp =
          "Action request does not contain structured property proposal")
  public void
      testAcceptStructuredPropertyProposalByInfoNoStructuredPropertyProposalThrowsMalformedActionRequest()
          throws Exception {
    // Given
    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType(ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL);
    // By default, the actionRequestInfo doesn't have params or structuredPropertyProposal

    // When
    actionRequestService.acceptStructuredPropertyProposal(mockOpContext, actionRequestInfo);

    // Then -> Expect MalformedActionRequestException
  }

  @Test
  public void testAcceptStructuredPropertyProposalWithSchemaFieldSuccess() throws Exception {

    AuditStamp auditStamp = new AuditStamp().setTime(1234L).setActor(TEST_ACTOR_URN);
    Mockito.when(mockOpContext.getAuditStamp()).thenReturn(auditStamp);

    // Given
    Urn actionRequestUrn = Urn.createFromString("urn:li:actionRequest:123");

    StructuredPropertyProposal proposal =
        new StructuredPropertyProposal()
            .setStructuredPropertyValues(
                new StructuredPropertyValueAssignmentArray(
                    ImmutableList.of(
                        new StructuredPropertyValueAssignment()
                            .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN_1)
                            .setValues(
                                new PrimitivePropertyValueArray(
                                    ImmutableList.of(
                                        PrimitivePropertyValue.create("testValue")))))));

    ActionRequestParams params = new ActionRequestParams();
    params.setStructuredPropertyProposal(proposal);

    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType(ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL);
    actionRequestInfo.setParams(params);
    actionRequestInfo.setResource("urn:li:dataset:123"); // entity URN
    actionRequestInfo.setSubResource("someSchemaField");

    EntityResponse response = new EntityResponse();
    response.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                Constants.ACTION_REQUEST_INFO_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(actionRequestInfo.data())))));

    // Set up the ActionRequestInfo as needed for the next method call
    doReturn(response)
        .when(mockEntityClient)
        .getV2(
            any(OperationContext.class),
            Mockito.eq(Constants.ACTION_REQUEST_ENTITY_NAME),
            Mockito.eq(actionRequestUrn),
            any());

    // When
    actionRequestService.acceptStructuredPropertyProposal(mockOpContext, actionRequestInfo);

    // Then
    // We verify that the structuredPropertyService.updateEntityStructuredProperties
    // was called with the URN for the schema field
    Urn expectedSchemaFieldUrn =
        UrnUtils.getUrn(
            String.format(
                "urn:li:schemaField:(%s,%s)",
                actionRequestInfo.getResource(), actionRequestInfo.getSubResource()));

    verify(mockStructuredPropertyService, times(1))
        .updateEntityStructuredProperties(
            eq(mockOpContext),
            eq(expectedSchemaFieldUrn),
            eq(proposal.getStructuredPropertyValues()));
  }

  @Test
  public void testAcceptStructuredPropertyProposalByInfoNoSchemaFieldSuccess() throws Exception {
    // Given
    String structuredPropertyProposalType = ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL;

    StructuredPropertyProposal proposal = new StructuredPropertyProposal();
    proposal.setStructuredPropertyValues(
        new StructuredPropertyValueAssignmentArray(
            Collections.singletonList(new StructuredPropertyValueAssignment())));

    ActionRequestParams params = new ActionRequestParams();
    params.setStructuredPropertyProposal(proposal);

    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType(structuredPropertyProposalType);
    actionRequestInfo.setParams(params);
    actionRequestInfo.setResource("urn:li:dataset:123"); // entity URN
    // subResource is null (no schema field)

    // When
    actionRequestService.acceptStructuredPropertyProposal(mockOpContext, actionRequestInfo);

    // Then
    Urn expectedEntityUrn = UrnUtils.getUrn(actionRequestInfo.getResource());
    verify(mockStructuredPropertyService, times(1))
        .updateEntityStructuredProperties(
            eq(mockOpContext), eq(expectedEntityUrn), any(List.class));
  }

  // ============================================================================
  // Utility class for mocking aspects in tests
  // ============================================================================
  private static class ActionRequestTestUtils {
    static Map<String, EnvelopedAspect> mockAspectsMapForActionRequest(
        ActionRequestInfo info, ActionRequestStatus status) {
      Map<String, EnvelopedAspect> aspectsMap = new HashMap<>();

      // ACTION_REQUEST_INFO_ASPECT
      aspectsMap.put(
          Constants.ACTION_REQUEST_INFO_ASPECT_NAME,
          new EnvelopedAspect()
              .setValue(new Aspect(info.data()))
              .setName(Constants.ACTION_REQUEST_INFO_ASPECT_NAME));

      // ACTION_REQUEST_STATUS_ASPECT
      if (status != null) {
        aspectsMap.put(
            Constants.ACTION_REQUEST_STATUS_ASPECT_NAME,
            new EnvelopedAspect()
                .setValue(new Aspect(status.data()))
                .setName(Constants.ACTION_REQUEST_STATUS_ASPECT_NAME));
      } else {
        // If no status is provided, let's default to an empty ActionRequestStatus
        aspectsMap.put(
            Constants.ACTION_REQUEST_STATUS_ASPECT_NAME,
            new EnvelopedAspect()
                .setValue(new Aspect(new ActionRequestStatus().data()))
                .setName(Constants.ACTION_REQUEST_STATUS_ASPECT_NAME));
      }
      return aspectsMap;
    }
  }

  @Test
  public void proposeCreateGlossaryNodeNullArguments() {
    assertThrows(
        () ->
            actionRequestService.proposeCreateGlossaryNode(
                mockOpContext, null, GLOSSARY_NODE_NAME, Optional.empty(), "test"));
    assertThrows(
        () ->
            actionRequestService.proposeCreateGlossaryNode(
                mockOpContext, ACTOR_URN, null, Optional.empty(), "test"));
    assertThrows(
        () ->
            actionRequestService.proposeCreateGlossaryNode(
                mockOpContext, ACTOR_URN, GLOSSARY_NODE_NAME, null, "test"));
  }

  @Test
  public void proposeCreateGlossaryNodePasses() {
    actionRequestService.proposeCreateGlossaryNode(
        mockOpContext, ACTOR_URN, GLOSSARY_NODE_NAME, Optional.empty(), "test");
    verify(mockEntityService).ingestEntity(any(OperationContext.class), any(), any());
  }

  @Test
  public void proposeCreateGlossaryTermNullArguments() {
    assertThrows(
        () ->
            actionRequestService.proposeCreateGlossaryTerm(
                mockOpContext, null, GLOSSARY_TERM_NAME, Optional.empty(), "test"));
    assertThrows(
        () ->
            actionRequestService.proposeCreateGlossaryTerm(
                mockOpContext, ACTOR_URN, null, Optional.empty(), "test"));
    assertThrows(
        () ->
            actionRequestService.proposeCreateGlossaryTerm(
                mockOpContext, ACTOR_URN, GLOSSARY_TERM_NAME, null, "test"));
  }

  @Test
  public void proposeCreateGlossaryTermPasses() {
    actionRequestService.proposeCreateGlossaryTerm(
        mockOpContext, ACTOR_URN, GLOSSARY_TERM_NAME, Optional.empty(), "test");
    verify(mockEntityService).ingestEntity(any(OperationContext.class), any(), any());
  }

  @Test
  public void proposeUpdateResourceDescriptionNullArguments() {
    assertThrows(
        () ->
            actionRequestService.proposeUpdateResourceDescription(
                mockOpContext, null, TEST_GLOSSARY_NODE_URN, null, null, DESCRIPTION));
    assertThrows(
        () ->
            actionRequestService.proposeUpdateResourceDescription(
                mockOpContext, ACTOR_URN, null, null, null, DESCRIPTION));
    assertThrows(
        () ->
            actionRequestService.proposeUpdateResourceDescription(
                mockOpContext, ACTOR_URN, TEST_GLOSSARY_NODE_URN, null, null, null));
  }

  private static class DescriptionUpdateActionRequestMatcher extends ActionRequestSnapshotMatcher {

    private final String description;

    private DescriptionUpdateActionRequestMatcher(
        String resourceUrn, String subResourceType, String subResource, String description) {
      super("UPDATE_DESCRIPTION", resourceUrn, subResourceType, subResource);
      this.description = description;
    }

    @Override
    boolean matchesActionRequestParams(ActionRequestParams params) {
      return params.hasUpdateDescriptionProposal()
          && Objects.equals(description, params.getUpdateDescriptionProposal().getDescription());
    }
  }

  @Test
  public void proposeUpdateResourceDescriptionPasses() throws RemoteInvocationException {
    when(mockEntityService.exists(
            any(OperationContext.class), eq(TEST_GLOSSARY_NODE_URN), anyBoolean()))
        .thenReturn(true);

    actionRequestService.proposeUpdateResourceDescription(
        mockOpContext, ACTOR_URN, TEST_GLOSSARY_NODE_URN, null, null, DESCRIPTION);

    DescriptionUpdateActionRequestMatcher snapshotMatcher =
        new DescriptionUpdateActionRequestMatcher(
            TEST_GLOSSARY_NODE_URN.toString(), null, null, DESCRIPTION);

    verify(mockEntityService)
        .ingestEntity(any(OperationContext.class), argThat(snapshotMatcher), any());
  }

  @Test
  public void proposeUpdateColumnDescriptionPasses()
      throws URISyntaxException, RemoteInvocationException {
    when(mockEntityService.exists(any(OperationContext.class), eq(TEST_ENTITY_URN), anyBoolean()))
        .thenReturn(true);

    EntitySpec spec = new EntitySpec(TEST_ENTITY_URN.getEntityType(), TEST_ENTITY_URN.toString());
    AuthorizedActors actors =
        new Authorizer() {}.authorizedActors(
            PoliciesConfig.MANAGE_ENTITY_DOCS_PROPOSALS_PRIVILEGE.getType(), Optional.of(spec));
    when(mockAuthorizer.authorizedActors(any(), any())).thenReturn(actors);

    actionRequestService.proposeUpdateResourceDescription(
        mockOpContext,
        ACTOR_URN,
        TEST_ENTITY_URN,
        SubResourceType.DATASET_FIELD.toString(),
        TEST_FIELD_PATH,
        DESCRIPTION);

    DescriptionUpdateActionRequestMatcher snapshotMatcher =
        new DescriptionUpdateActionRequestMatcher(
            TEST_ENTITY_URN.toString(),
            SubResourceType.DATASET_FIELD.toString(),
            TEST_FIELD_PATH,
            DESCRIPTION);

    verify(mockEntityService)
        .ingestEntity(any(OperationContext.class), argThat(snapshotMatcher), any());
  }

  @Test
  public void proposeUpdateDescriptionErrPartialSpec() throws URISyntaxException {
    // error should occur when only one of subResourceType or subResource are specified
    Urn datasetUrn =
        Urn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-dataset.user-table,PROD)");

    String fieldPath = "someField";

    when(mockEntityService.exists(any(OperationContext.class), eq(datasetUrn), anyBoolean()))
        .thenReturn(true);

    EntitySpec spec = new EntitySpec(datasetUrn.getEntityType(), datasetUrn.toString());
    AuthorizedActors actors =
        new Authorizer() {}.authorizedActors(
            PoliciesConfig.MANAGE_ENTITY_DOCS_PROPOSALS_PRIVILEGE.getType(), Optional.of(spec));
    when(mockAuthorizer.authorizedActors(any(), any())).thenReturn(actors);

    DescriptionUpdateActionRequestMatcher snapshotMatcher =
        new DescriptionUpdateActionRequestMatcher(
            datasetUrn.toString(),
            SubResourceType.DATASET_FIELD.toString(),
            fieldPath,
            DESCRIPTION);

    assertThrows(
        () ->
            actionRequestService.proposeUpdateResourceDescription(
                mockOpContext,
                ACTOR_URN,
                datasetUrn,
                SubResourceType.DATASET_FIELD.toString(),
                null,
                DESCRIPTION));

    assertThrows(
        () ->
            actionRequestService.proposeUpdateResourceDescription(
                mockOpContext, ACTOR_URN, datasetUrn, null, fieldPath, DESCRIPTION));

    verify(mockEntityService, never()).ingestEntity(any(OperationContext.class), any(), any());
  }

  @Test
  public void isAuthorizedToResolveGlossaryEntityAsOwnerNullArguments() {
    assertThrows(
        () ->
            actionRequestService.isAuthorizedToResolveGlossaryEntityAsOwner(
                mockOpContext, null, Optional.of(TEST_GLOSSARY_NODE_URN)));
  }

  @Test
  public void isAuthorizedToResolveGlossaryEntityAsOwnerEmptyParentNode()
      throws RemoteInvocationException, URISyntaxException {
    assertFalse(
        actionRequestService.isAuthorizedToResolveGlossaryEntityAsOwner(
            mockOpContext, ACTOR_URN, Optional.empty()));
  }

  @Test
  public void isAuthorizedToResolveGlossaryEntityAsOwnerActorIsOwnerPasses()
      throws RemoteInvocationException, URISyntaxException {
    Ownership ownership =
        new Ownership()
            .setOwners(new OwnerArray(ImmutableList.of(new Owner().setOwner(ACTOR_URN))));
    when(mockEntityService.getLatestAspect(
            any(OperationContext.class), eq(TEST_GLOSSARY_NODE_URN), eq(OWNERSHIP_ASPECT_NAME)))
        .thenReturn(ownership);

    assertTrue(
        actionRequestService.isAuthorizedToResolveGlossaryEntityAsOwner(
            mockOpContext, ACTOR_URN, Optional.of(TEST_GLOSSARY_NODE_URN)));
  }

  @Test
  public void isAuthorizedToResolveGlossaryEntityAsOwnerActorIsInOwnerGroupPasses()
      throws RemoteInvocationException, URISyntaxException {
    Ownership ownership =
        new Ownership()
            .setOwners(new OwnerArray(ImmutableList.of(new Owner().setOwner(GROUP_URN))));
    GroupMembership groupMembership =
        new GroupMembership().setGroups(new UrnArray(Collections.singletonList(GROUP_URN)));

    when(mockEntityService.getLatestAspect(
            any(OperationContext.class), eq(TEST_GLOSSARY_NODE_URN), eq(OWNERSHIP_ASPECT_NAME)))
        .thenReturn(ownership);
    when(mockEntityService.getLatestAspect(
            any(OperationContext.class), eq(ACTOR_URN), eq(GROUP_MEMBERSHIP_ASPECT_NAME)))
        .thenReturn(groupMembership);

    assertTrue(
        actionRequestService.isAuthorizedToResolveGlossaryEntityAsOwner(
            mockOpContext, ACTOR_URN, Optional.of(TEST_GLOSSARY_NODE_URN)));
  }

  @Test
  public void acceptCreateGlossaryNodeProposalNullArguments() {
    assertThrows(
        () ->
            actionRequestService.acceptCreateGlossaryNodeProposal(
                mockOpContext, null, new ActionRequestSnapshot(), true));
    assertThrows(
        () ->
            actionRequestService.acceptCreateGlossaryNodeProposal(
                mockOpContext, ACTOR_URN, null, true));
  }

  @Test
  public void acceptCreateGlossaryNodeProposalPasses() throws Exception {
    when(mockEntityClient.exists(any(), any())).thenReturn(false);

    ActionRequestSnapshot actionRequestSnapshot =
        actionRequestService.createCreateGlossaryNodeProposalActionRequest(
            ACTOR_URN,
            Collections.singletonList(ACTOR_URN),
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            GLOSSARY_NODE_NAME,
            Optional.empty(),
            "test");
    actionRequestService.acceptCreateGlossaryNodeProposal(
        mockOpContext, ACTOR_URN, actionRequestSnapshot, true);

    verify(mockEntityClient).ingestProposal(any(), any());
  }

  @Test
  public void acceptCreateGlossaryTermProposalNullArguments() {
    assertThrows(
        () ->
            actionRequestService.acceptCreateGlossaryTermProposal(
                mockOpContext, null, new ActionRequestSnapshot(), true));
    assertThrows(
        () ->
            actionRequestService.acceptCreateGlossaryTermProposal(
                mockOpContext, ACTOR_URN, null, true));
  }

  @Test
  public void acceptCreateGlossaryTermProposalPasses() throws Exception {
    when(mockEntityClient.exists(any(), any())).thenReturn(false);

    ActionRequestSnapshot actionRequestSnapshot =
        actionRequestService.createCreateGlossaryTermProposalActionRequest(
            ACTOR_URN,
            Collections.singletonList(ACTOR_URN),
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            GLOSSARY_TERM_NAME,
            Optional.empty(),
            "test");
    actionRequestService.acceptCreateGlossaryTermProposal(
        mockOpContext, ACTOR_URN, actionRequestSnapshot, true);

    verify(mockEntityClient).ingestProposal(any(), any());
  }

  @Test
  public void acceptUpdateResourceDescriptionProposalNullArguments() {
    assertThrows(
        () -> actionRequestService.acceptUpdateResourceDescriptionProposal(mockOpContext, null));
    assertThrows(
        () ->
            actionRequestService.acceptUpdateResourceDescriptionProposal(
                mockOpContext, new ActionRequestSnapshot()));
  }

  @Test
  public void acceptUpdateResourceDescriptionProposalForGlossaryNodePasses() throws Exception {
    GlossaryNodeInfo glossaryNodeInfo =
        actionRequestService.mapGlossaryNodeInfo(
            GLOSSARY_NODE_NAME, Optional.empty(), Optional.of("test"));
    when(mockEntityClient.exists(any(), any())).thenReturn(false);
    when(mockEntityService.getLatestAspect(
            any(OperationContext.class),
            eq(TEST_GLOSSARY_NODE_URN),
            eq(GLOSSARY_NODE_INFO_ASPECT_NAME)))
        .thenReturn(glossaryNodeInfo);

    ActionRequestSnapshot actionRequestSnapshot =
        actionRequestService.createUpdateDescriptionProposalActionRequest(
            ACTOR_URN,
            TEST_GLOSSARY_NODE_URN,
            null,
            null,
            Collections.singletonList(ACTOR_URN),
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            DESCRIPTION);
    actionRequestService.acceptUpdateResourceDescriptionProposal(
        mockOpContext, actionRequestSnapshot);

    verify(mockEntityClient).ingestProposal(any(), any());
  }

  @Test
  public void acceptUpdateResourceDescriptionProposalForGlossaryTermPasses() throws Exception {
    GlossaryTermInfo glossaryTermInfo =
        actionRequestService.mapGlossaryTermInfo(
            GLOSSARY_TERM_NAME, Optional.empty(), Optional.of("test"));
    when(mockEntityClient.exists(any(), any())).thenReturn(false);
    when(mockEntityService.getLatestAspect(
            any(OperationContext.class),
            eq(TEST_GLOSSARY_TERM_URN),
            eq(GLOSSARY_TERM_INFO_ASPECT_NAME)))
        .thenReturn(glossaryTermInfo);

    ActionRequestSnapshot actionRequestSnapshot =
        actionRequestService.createUpdateDescriptionProposalActionRequest(
            ACTOR_URN,
            TEST_GLOSSARY_TERM_URN,
            null,
            null,
            Collections.singletonList(ACTOR_URN),
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            DESCRIPTION);
    actionRequestService.acceptUpdateResourceDescriptionProposal(
        mockOpContext, actionRequestSnapshot);

    verify(mockEntityClient).ingestProposal(any(), any());
  }

  private static class DatasetDescriptionMatcher extends MetadataChangeProposalMatcher {
    private final String description;

    private DatasetDescriptionMatcher(
        Urn urn, String entityType, String aspectName, ChangeType changeType, String description) {
      super(urn, entityType, aspectName, changeType);
      this.description = description;
    }

    @Override
    boolean matchesAspect(GenericAspect aspect) {
      EditableDatasetProperties editableDatasetProperties =
          GenericRecordUtils.deserializeAspect(
              aspect.getValue(), GenericRecordUtils.JSON, EditableDatasetProperties.class);

      return Objects.equals(description, editableDatasetProperties.getDescription());
    }
  }

  @Test
  public void acceptUpdateDatasetDescriptionProposalNewAspectPasses() throws Exception {
    when(mockEntityService.getLatestAspect(
            any(OperationContext.class),
            eq(TEST_ENTITY_URN),
            eq(EDITABLE_DATASET_PROPERTIES_ASPECT_NAME)))
        .thenReturn(null);

    ActionRequestSnapshot actionRequestSnapshot =
        actionRequestService.createUpdateDescriptionProposalActionRequest(
            ACTOR_URN,
            TEST_ENTITY_URN,
            null,
            null,
            Collections.singletonList(ACTOR_URN),
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            DESCRIPTION);

    actionRequestService.acceptUpdateResourceDescriptionProposal(
        mockOpContext, actionRequestSnapshot);

    DatasetDescriptionMatcher datasetDescriptionMatcher =
        new DatasetDescriptionMatcher(
            TEST_ENTITY_URN,
            DATASET_ENTITY_NAME,
            EDITABLE_DATASET_PROPERTIES_ASPECT_NAME,
            ChangeType.UPSERT,
            DESCRIPTION);

    verify(mockEntityClient)
        .ingestProposal(any(OperationContext.class), argThat(datasetDescriptionMatcher));
  }

  @Test
  public void acceptUpdateDatasetDescriptionProposalExistingAspectPasses() throws Exception {
    EditableDatasetProperties editableDatasetProperties = new EditableDatasetProperties();
    editableDatasetProperties.setDescription("OLD_" + DESCRIPTION);

    when(mockEntityService.getLatestAspect(
            any(OperationContext.class),
            eq(TEST_ENTITY_URN),
            eq(EDITABLE_DATASET_PROPERTIES_ASPECT_NAME)))
        .thenReturn(editableDatasetProperties);

    ActionRequestSnapshot actionRequestSnapshot =
        actionRequestService.createUpdateDescriptionProposalActionRequest(
            ACTOR_URN,
            TEST_ENTITY_URN,
            null,
            null,
            Collections.singletonList(ACTOR_URN),
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            DESCRIPTION);

    actionRequestService.acceptUpdateResourceDescriptionProposal(
        mockOpContext, actionRequestSnapshot);

    DatasetDescriptionMatcher datasetDescriptionMatcher =
        new DatasetDescriptionMatcher(
            TEST_ENTITY_URN,
            DATASET_ENTITY_NAME,
            EDITABLE_DATASET_PROPERTIES_ASPECT_NAME,
            ChangeType.UPSERT,
            DESCRIPTION);

    verify(mockEntityClient)
        .ingestProposal(any(OperationContext.class), argThat(datasetDescriptionMatcher));
  }

  private static class ColumnDescriptionMatcher extends MetadataChangeProposalMatcher {
    private final String description;

    private ColumnDescriptionMatcher(
        Urn urn, String entityType, String aspectName, ChangeType changeType, String description) {
      super(urn, entityType, aspectName, changeType);
      this.description = description;
    }

    @Override
    boolean matchesAspect(GenericAspect aspect) {
      EditableSchemaMetadata editableSchemaMetadata =
          GenericRecordUtils.deserializeAspect(
              aspect.getValue(), GenericRecordUtils.JSON, EditableSchemaMetadata.class);

      EditableSchemaFieldInfo editableSchemaFieldInfo =
          DescriptionUtils.getFieldInfoFromSchema(editableSchemaMetadata, TEST_FIELD_PATH);

      return Objects.equals(description, editableSchemaFieldInfo.getDescription());
    }
  }

  @Test
  public void acceptUpdateColumnDescriptionProposalNewAspectPasses() throws Exception {
    when(mockEntityService.getLatestAspect(
            any(OperationContext.class),
            eq(TEST_ENTITY_URN),
            eq(EDITABLE_SCHEMA_METADATA_ASPECT_NAME)))
        .thenReturn(null);

    ActionRequestSnapshot actionRequestSnapshot =
        actionRequestService.createUpdateDescriptionProposalActionRequest(
            ACTOR_URN,
            TEST_ENTITY_URN,
            SubResourceType.DATASET_FIELD.toString(),
            TEST_FIELD_PATH,
            Collections.singletonList(ACTOR_URN),
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            DESCRIPTION);

    actionRequestService.acceptUpdateResourceDescriptionProposal(
        mockOpContext, actionRequestSnapshot);

    ColumnDescriptionMatcher columnDescriptionMatcher =
        new ColumnDescriptionMatcher(
            TEST_ENTITY_URN,
            DATASET_ENTITY_NAME,
            EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
            ChangeType.UPSERT,
            DESCRIPTION);

    verify(mockEntityClient)
        .ingestProposal(any(OperationContext.class), argThat(columnDescriptionMatcher));
  }

  @Test
  public void acceptUpdateColumnDescriptionProposalExistingAspectPasses() throws Exception {
    EditableSchemaMetadata editableSchemaMetadata = new EditableSchemaMetadata();
    EditableSchemaFieldInfo editableSchemaFieldInfo = new EditableSchemaFieldInfo();
    editableSchemaMetadata.setEditableSchemaFieldInfo(
        new EditableSchemaFieldInfoArray(editableSchemaFieldInfo));
    editableSchemaFieldInfo.setDescription("OLD_" + DESCRIPTION);
    editableSchemaFieldInfo.setFieldPath(TEST_FIELD_PATH);

    when(mockEntityService.getLatestAspect(
            any(OperationContext.class),
            eq(TEST_ENTITY_URN),
            eq(EDITABLE_SCHEMA_METADATA_ASPECT_NAME)))
        .thenReturn(editableSchemaMetadata);

    ActionRequestSnapshot actionRequestSnapshot =
        actionRequestService.createUpdateDescriptionProposalActionRequest(
            ACTOR_URN,
            TEST_ENTITY_URN,
            SubResourceType.DATASET_FIELD.toString(),
            TEST_FIELD_PATH,
            Collections.singletonList(ACTOR_URN),
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            DESCRIPTION);

    actionRequestService.acceptUpdateResourceDescriptionProposal(
        mockOpContext, actionRequestSnapshot);

    ColumnDescriptionMatcher columnDescriptionMatcher =
        new ColumnDescriptionMatcher(
            TEST_ENTITY_URN,
            DATASET_ENTITY_NAME,
            EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
            ChangeType.UPSERT,
            DESCRIPTION);

    verify(mockEntityClient)
        .ingestProposal(any(OperationContext.class), argThat(columnDescriptionMatcher));
  }

  @Test
  public void acceptUpdateColumnDescriptionProposalNoSubResourceFails() throws Exception {
    when(mockEntityService.getLatestAspect(
            any(OperationContext.class),
            eq(TEST_ENTITY_URN),
            eq(EDITABLE_SCHEMA_METADATA_ASPECT_NAME)))
        .thenReturn(null);

    ActionRequestSnapshot actionRequestSnapshot =
        actionRequestService.createUpdateDescriptionProposalActionRequest(
            ACTOR_URN,
            TEST_ENTITY_URN,
            SubResourceType.DATASET_FIELD.toString(),
            TEST_FIELD_PATH,
            Collections.singletonList(ACTOR_URN),
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            DESCRIPTION);

    // set subresource to null
    actionRequestSnapshot.getAspects().get(1).getActionRequestInfo().removeSubResource();
    assertThrows(
        NullPointerException.class,
        () ->
            actionRequestService.acceptUpdateResourceDescriptionProposal(
                mockOpContext, actionRequestSnapshot));

    verify(mockEntityClient, never()).ingestProposal(any(), any());
  }

  @Test
  public void completeProposalNullArguments() {
    assertThrows(
        () ->
            actionRequestService.completeProposal(
                mockOpContext,
                null,
                ACTION_REQUEST_STATUS_COMPLETE,
                ACTION_REQUEST_RESULT_ACCEPTED,
                null,
                new Entity()));
    assertThrows(
        () ->
            actionRequestService.completeProposal(
                mockOpContext,
                ACTOR_URN,
                null,
                ACTION_REQUEST_RESULT_ACCEPTED,
                null,
                new Entity()));
    assertThrows(
        () ->
            actionRequestService.completeProposal(
                mockOpContext,
                ACTOR_URN,
                ACTION_REQUEST_STATUS_COMPLETE,
                null,
                null,
                new Entity()));
    assertThrows(
        () ->
            actionRequestService.completeProposal(
                mockOpContext,
                ACTOR_URN,
                ACTION_REQUEST_STATUS_COMPLETE,
                ACTION_REQUEST_RESULT_ACCEPTED,
                null,
                null));
  }

  @Test
  public void completeProposal() {
    ActionRequestSnapshot actionRequestSnapshot =
        actionRequestService.createUpdateDescriptionProposalActionRequest(
            ACTOR_URN,
            TEST_GLOSSARY_NODE_URN,
            null,
            null,
            Collections.singletonList(ACTOR_URN),
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            DESCRIPTION);
    Entity entity = new Entity().setValue(Snapshot.create(actionRequestSnapshot));
    actionRequestService.completeProposal(
        mockOpContext,
        ACTOR_URN,
        ACTION_REQUEST_STATUS_COMPLETE,
        ACTION_REQUEST_RESULT_ACCEPTED,
        null,
        entity);

    verify(mockEntityService).ingestEntity(any(OperationContext.class), any(), any());
  }

  @Test
  public void completeProposalWithNote() {
    ActionRequestSnapshot actionRequestSnapshot =
        actionRequestService.createUpdateDescriptionProposalActionRequest(
            ACTOR_URN,
            TEST_GLOSSARY_NODE_URN,
            null,
            null,
            Collections.singletonList(ACTOR_URN),
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            DESCRIPTION);
    Entity entity = new Entity().setValue(Snapshot.create(actionRequestSnapshot));

    actionRequestService.completeProposal(
        mockOpContext,
        ACTOR_URN,
        ACTION_REQUEST_STATUS_COMPLETE,
        ACTION_REQUEST_RESULT_ACCEPTED,
        "Test Note",
        entity);

    ArgumentCaptor<Entity> entityCaptor = ArgumentCaptor.forClass(Entity.class);

    verify(mockEntityService, times(1))
        .ingestEntity(any(OperationContext.class), entityCaptor.capture(), any());

    Entity actualEntity = entityCaptor.getValue();
    ActionRequestStatus actualStatus =
        actualEntity.getValue().getActionRequestSnapshot().getAspects().stream()
            .filter(aspect -> aspect.isActionRequestStatus())
            .findFirst()
            .get()
            .getActionRequestStatus();

    assertEquals(actualStatus.getStatus(), ACTION_REQUEST_STATUS_COMPLETE);
    assertEquals(actualStatus.getNote(), "Test Note");
    assertEquals(actualStatus.getResult(), ACTION_REQUEST_RESULT_ACCEPTED);
  }

  private abstract static class ActionRequestSnapshotMatcher implements ArgumentMatcher<Entity> {

    private final String infoType;
    private final String resourceUrn;
    private final String subResourceType;
    private final String subResource;

    private ActionRequestSnapshotMatcher(
        String infoType, String resourceUrn, String subResourceType, String subResource) {
      this.infoType = infoType;
      this.resourceUrn = resourceUrn;
      this.subResourceType = subResourceType;
      this.subResource = subResource;
    }

    @Override
    public boolean matches(Entity entity) {
      Snapshot snapshot = entity.getValue();
      if (!snapshot.isActionRequestSnapshot()) {
        return false;
      }

      for (ActionRequestAspect aspect : snapshot.getActionRequestSnapshot().getAspects()) {
        if (aspect.isActionRequestInfo()) {
          ActionRequestInfo actual = aspect.getActionRequestInfo();
          return Objects.equals(infoType, actual.getType())
              && Objects.equals(resourceUrn, actual.getResource())
              && Objects.equals(subResourceType, actual.getSubResourceType())
              && Objects.equals(subResource, actual.getSubResource())
              && matchesActionRequestParams(actual.getParams());
        }
      }

      return false;
    }

    abstract boolean matchesActionRequestParams(ActionRequestParams params);
  }

  private abstract static class MetadataChangeProposalMatcher
      implements ArgumentMatcher<MetadataChangeProposal> {

    private final Urn urn;
    private final String entityType;
    private final String aspectName;
    private final ChangeType changeType;

    private MetadataChangeProposalMatcher(
        Urn urn, String entityType, String aspectName, ChangeType changeType) {
      this.urn = urn;
      this.entityType = entityType;
      this.aspectName = aspectName;
      this.changeType = changeType;
    }

    @Override
    public boolean matches(MetadataChangeProposal mcp) {
      return Objects.equals(urn, mcp.getEntityUrn())
          && Objects.equals(entityType, mcp.getEntityType())
          && Objects.equals(aspectName, mcp.getAspectName())
          && Objects.equals(changeType, mcp.getChangeType())
          && matchesAspect(mcp.getAspect());
    }

    abstract boolean matchesAspect(GenericAspect aspect);
  }
}
