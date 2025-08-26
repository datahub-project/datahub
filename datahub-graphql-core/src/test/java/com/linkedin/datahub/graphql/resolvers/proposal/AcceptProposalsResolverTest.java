package com.linkedin.datahub.graphql.resolvers.proposal;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.ACCEPTED_ACTION_REQUEST;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.DESCRIPTION_ACTION_REQUEST;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.DOMAIN_ACTION_REQUEST;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.LEGACY_TAG_ACTION_REQUEST;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.LEGACY_TERM_ACTION_REQUEST;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.OWNER_ACTION_REQUEST;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.STRUCTURED_PROPERTY_ACTION_REQUEST;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.TAG_ACTION_REQUEST;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.TERM_ACTION_REQUEST;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.TEST_ACTOR_URN;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.TEST_GLOSSARY_TERM;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.TEST_GLOSSARY_TERM_2;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.TEST_TAG;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.TEST_TAG_2;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.buildLegacyMockTagProposalSnapshot;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.buildMockAcceptedSnapshot;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.buildMockDocumentationSnapshot;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.buildMockDomainSnapshot;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.buildMockLegacyTermProposalSnapshot;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.buildMockOwnerSnapshot;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.buildMockStructuredPropertySnapshot;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.buildMockTagProposalSnapshot;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.buildMockTermProposalSnapshot;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_RESULT_ACCEPTED;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_STATUS_COMPLETE;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.ActionRequestService;
import com.linkedin.metadata.snapshot.ActionRequestSnapshot;
import com.linkedin.metadata.utils.GenericRecordUtils;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AcceptProposalsResolverTest {

  @Test
  public void testAcceptProposalsInvalidUrns() throws Exception {
    Set<String> badProposalsUrn = ImmutableSet.of("urn:li:action:123");

    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ActionRequestService mockActionRequestService = Mockito.mock(ActionRequestService.class);
    AcceptProposalsResolver resolver =
        new AcceptProposalsResolver(mockEntityService, mockActionRequestService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urns"))).thenReturn(badProposalsUrn);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Assert.assertThrows(ExecutionException.class, () -> resolver.get(mockEnv).get());
  }

  @Test
  public void testAcceptProposalsSuccessLegacyTags() throws Exception {
    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ActionRequestService mockActionRequestService = Mockito.mock(ActionRequestService.class);

    AcceptProposalsResolver resolver =
        new AcceptProposalsResolver(mockEntityService, mockActionRequestService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(ImmutableList.of(LEGACY_TAG_ACTION_REQUEST.toString()));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).join();

    // Basic check to verify that the tag change went through.
    Mockito.verify(mockEntityService, Mockito.times(1))
        .ingestProposal(
            Mockito.any(OperationContext.class),
            Mockito.any(AspectsBatch.class),
            Mockito.eq(false));

    // Verify the proposal was marked as completed
    Mockito.verify(mockActionRequestService, Mockito.times(1))
        .completeProposal(
            Mockito.any(OperationContext.class),
            Mockito.eq(TEST_ACTOR_URN),
            Mockito.eq(ACTION_REQUEST_STATUS_COMPLETE),
            Mockito.eq(ACTION_REQUEST_RESULT_ACCEPTED),
            Mockito.eq(null),
            Mockito.any(Entity.class));
  }

  @Test
  public void testAcceptProposalsSuccessTags() throws Exception {
    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ActionRequestService mockActionRequestService = Mockito.mock(ActionRequestService.class);

    AcceptProposalsResolver resolver =
        new AcceptProposalsResolver(mockEntityService, mockActionRequestService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(ImmutableList.of(TAG_ACTION_REQUEST.toString()));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).join();

    ArgumentCaptor<AspectsBatch> aspectsBatchCaptor = ArgumentCaptor.forClass(AspectsBatch.class);

    verify(mockEntityService, times(1))
        .ingestProposal(any(OperationContext.class), aspectsBatchCaptor.capture(), eq(false));

    AspectsBatch capturedAspectsBatch = aspectsBatchCaptor.getValue();
    assertNotNull(capturedAspectsBatch);

    GlobalTags actualTags =
        GenericRecordUtils.deserializeAspect(
            capturedAspectsBatch
                .getMCPItems()
                .get(0)
                .getMetadataChangeProposal()
                .getAspect()
                .getValue(),
            capturedAspectsBatch
                .getMCPItems()
                .get(0)
                .getMetadataChangeProposal()
                .getAspect()
                .getContentType(),
            GlobalTags.class);

    // Validate that the correct tags were added.
    assertEquals(actualTags.getTags().get(0).getTag().toString(), TEST_TAG.toString());
    assertEquals(actualTags.getTags().get(1).getTag().toString(), TEST_TAG_2.toString());

    // Verify the proposal was marked as completed
    Mockito.verify(mockActionRequestService, Mockito.times(1))
        .completeProposal(
            Mockito.any(OperationContext.class),
            Mockito.eq(TEST_ACTOR_URN),
            Mockito.eq(ACTION_REQUEST_STATUS_COMPLETE),
            Mockito.eq(ACTION_REQUEST_RESULT_ACCEPTED),
            Mockito.eq(null),
            Mockito.any(Entity.class));
  }

  @Test
  public void testAcceptProposalsSuccessLegacyTerms() throws Exception {
    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ActionRequestService mockActionRequestService = Mockito.mock(ActionRequestService.class);

    AcceptProposalsResolver resolver =
        new AcceptProposalsResolver(mockEntityService, mockActionRequestService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(ImmutableList.of(LEGACY_TERM_ACTION_REQUEST.toString()));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).join();

    // Basic check to verify that the tag change went through.
    Mockito.verify(mockEntityService, Mockito.times(1))
        .ingestProposal(
            Mockito.any(OperationContext.class),
            Mockito.any(AspectsBatch.class),
            Mockito.eq(false));

    // Verify the proposal was marked as completed
    Mockito.verify(mockActionRequestService, Mockito.times(1))
        .completeProposal(
            Mockito.any(OperationContext.class),
            Mockito.eq(TEST_ACTOR_URN),
            Mockito.eq(ACTION_REQUEST_STATUS_COMPLETE),
            Mockito.eq(ACTION_REQUEST_RESULT_ACCEPTED),
            Mockito.eq(null),
            Mockito.any(Entity.class));
  }

  @Test
  public void testAcceptProposalsSuccessTerms() throws Exception {
    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ActionRequestService mockActionRequestService = Mockito.mock(ActionRequestService.class);

    AcceptProposalsResolver resolver =
        new AcceptProposalsResolver(mockEntityService, mockActionRequestService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(ImmutableList.of(TERM_ACTION_REQUEST.toString()));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).join();

    ArgumentCaptor<AspectsBatch> aspectsBatchCaptor = ArgumentCaptor.forClass(AspectsBatch.class);

    verify(mockEntityService, times(1))
        .ingestProposal(any(OperationContext.class), aspectsBatchCaptor.capture(), eq(false));

    AspectsBatch capturedAspectsBatch = aspectsBatchCaptor.getValue();
    assertNotNull(capturedAspectsBatch);

    GlossaryTerms actualTerms =
        GenericRecordUtils.deserializeAspect(
            capturedAspectsBatch
                .getMCPItems()
                .get(0)
                .getMetadataChangeProposal()
                .getAspect()
                .getValue(),
            capturedAspectsBatch
                .getMCPItems()
                .get(0)
                .getMetadataChangeProposal()
                .getAspect()
                .getContentType(),
            GlossaryTerms.class);

    // Validate that the correct terms were added.
    assertEquals(actualTerms.getTerms().get(0).getUrn().toString(), TEST_GLOSSARY_TERM.toString());
    assertEquals(
        actualTerms.getTerms().get(1).getUrn().toString(), TEST_GLOSSARY_TERM_2.toString());

    // Verify the proposal was marked as completed
    Mockito.verify(mockActionRequestService, Mockito.times(1))
        .completeProposal(
            Mockito.any(OperationContext.class),
            Mockito.eq(TEST_ACTOR_URN),
            Mockito.eq(ACTION_REQUEST_STATUS_COMPLETE),
            Mockito.eq(ACTION_REQUEST_RESULT_ACCEPTED),
            Mockito.eq(null),
            Mockito.any(Entity.class));
  }

  @Test
  public void testAcceptProposalsSuccessUpdateDescription() throws Exception {
    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ActionRequestService mockActionRequestService = Mockito.mock(ActionRequestService.class);

    AcceptProposalsResolver resolver =
        new AcceptProposalsResolver(mockEntityService, mockActionRequestService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(ImmutableList.of(DESCRIPTION_ACTION_REQUEST.toString()));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).join();

    // Basic check to verify that the tag change went through.
    Mockito.verify(mockActionRequestService, Mockito.times(1))
        .acceptUpdateResourceDescriptionProposal(
            Mockito.any(OperationContext.class), Mockito.any(ActionRequestSnapshot.class));

    // Verify the proposal was marked as completed
    Mockito.verify(mockActionRequestService, Mockito.times(1))
        .completeProposal(
            Mockito.any(OperationContext.class),
            Mockito.eq(TEST_ACTOR_URN),
            Mockito.eq(ACTION_REQUEST_STATUS_COMPLETE),
            Mockito.eq(ACTION_REQUEST_RESULT_ACCEPTED),
            Mockito.eq(null),
            Mockito.any(Entity.class));
  }

  @Test
  public void testAcceptProposalsSuccessMultiple() throws Exception {
    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ActionRequestService mockActionRequestService = Mockito.mock(ActionRequestService.class);

    AcceptProposalsResolver resolver =
        new AcceptProposalsResolver(mockEntityService, mockActionRequestService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(
            ImmutableList.of(
                LEGACY_TAG_ACTION_REQUEST.toString(), LEGACY_TERM_ACTION_REQUEST.toString()));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).join();

    // Basic check to verify that the tag change went through.
    Mockito.verify(mockEntityService, Mockito.times(2))
        .ingestProposal(
            Mockito.any(OperationContext.class),
            Mockito.any(AspectsBatch.class),
            Mockito.eq(false));

    // Verify the proposal was marked as completed
    Mockito.verify(mockActionRequestService, Mockito.times(2))
        .completeProposal(
            Mockito.any(OperationContext.class),
            Mockito.eq(TEST_ACTOR_URN),
            Mockito.eq(ACTION_REQUEST_STATUS_COMPLETE),
            Mockito.eq(ACTION_REQUEST_RESULT_ACCEPTED),
            Mockito.eq(null),
            Mockito.any(Entity.class));
  }

  @Test
  public void testAcceptProposalsSuccessStructuredProperties() throws Exception {
    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ActionRequestService mockActionRequestService = Mockito.mock(ActionRequestService.class);

    AcceptProposalsResolver resolver =
        new AcceptProposalsResolver(mockEntityService, mockActionRequestService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(ImmutableList.of(STRUCTURED_PROPERTY_ACTION_REQUEST.toString()));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).join();

    Mockito.verify(mockActionRequestService, Mockito.times(1))
        .acceptStructuredPropertyProposal(
            Mockito.any(OperationContext.class), Mockito.any(ActionRequestInfo.class));

    // Verify the proposal was marked as completed
    Mockito.verify(mockActionRequestService, Mockito.times(1))
        .completeProposal(
            Mockito.any(OperationContext.class),
            Mockito.eq(TEST_ACTOR_URN),
            Mockito.eq(ACTION_REQUEST_STATUS_COMPLETE),
            Mockito.eq(ACTION_REQUEST_RESULT_ACCEPTED),
            Mockito.eq(null),
            Mockito.any(Entity.class));
  }

  @Test
  public void testAcceptProposalsSuccessDomain() throws Exception {
    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ActionRequestService mockActionRequestService = Mockito.mock(ActionRequestService.class);

    AcceptProposalsResolver resolver =
        new AcceptProposalsResolver(mockEntityService, mockActionRequestService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(ImmutableList.of(DOMAIN_ACTION_REQUEST.toString()));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).join();

    Mockito.verify(mockActionRequestService, Mockito.times(1))
        .acceptDomainProposal(
            Mockito.any(OperationContext.class), Mockito.any(ActionRequestInfo.class));

    // Verify the proposal was marked as completed
    Mockito.verify(mockActionRequestService, Mockito.times(1))
        .completeProposal(
            Mockito.any(OperationContext.class),
            Mockito.eq(TEST_ACTOR_URN),
            Mockito.eq(ACTION_REQUEST_STATUS_COMPLETE),
            Mockito.eq(ACTION_REQUEST_RESULT_ACCEPTED),
            Mockito.eq(null),
            Mockito.any(Entity.class));
  }

  @Test
  public void testAcceptProposalsSuccessOwners() throws Exception {
    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ActionRequestService mockActionRequestService = Mockito.mock(ActionRequestService.class);

    AcceptProposalsResolver resolver =
        new AcceptProposalsResolver(mockEntityService, mockActionRequestService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(ImmutableList.of(OWNER_ACTION_REQUEST.toString()));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).join();

    ActionRequestSnapshot snapshot =
        buildMockOwnerSnapshot(OWNER_ACTION_REQUEST).getActionRequestSnapshot();
    ActionRequestInfo expectedActionRequestInfo =
        snapshot.getAspects().stream()
            .filter(aspect -> aspect.isActionRequestInfo())
            .findFirst()
            .get()
            .getActionRequestInfo();

    Mockito.verify(mockActionRequestService, Mockito.times(1))
        .acceptOwnerProposal(
            Mockito.any(OperationContext.class), Mockito.eq(expectedActionRequestInfo));

    // Verify the proposal was marked as completed
    Mockito.verify(mockActionRequestService, Mockito.times(1))
        .completeProposal(
            Mockito.any(OperationContext.class),
            Mockito.eq(TEST_ACTOR_URN),
            Mockito.eq(ACTION_REQUEST_STATUS_COMPLETE),
            Mockito.eq(ACTION_REQUEST_RESULT_ACCEPTED),
            Mockito.eq(null),
            Mockito.any(Entity.class));
  }

  @Test
  public void testAcceptProposalSuccessWithNote() throws Exception {
    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ActionRequestService mockActionRequestService = Mockito.mock(ActionRequestService.class);

    AcceptProposalsResolver resolver =
        new AcceptProposalsResolver(mockEntityService, mockActionRequestService);

    String testNote = "Test Note";

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(ImmutableList.of(TAG_ACTION_REQUEST.toString()));
    Mockito.when(mockEnv.getArgument(Mockito.eq("note"))).thenReturn(testNote);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).join();

    // Verify the proposal was marked as completed with the test note.
    Mockito.verify(mockActionRequestService, Mockito.times(1))
        .completeProposal(
            Mockito.any(OperationContext.class),
            Mockito.eq(TEST_ACTOR_URN),
            Mockito.eq(ACTION_REQUEST_STATUS_COMPLETE),
            Mockito.eq(ACTION_REQUEST_RESULT_ACCEPTED),
            Mockito.eq(testNote),
            Mockito.any(Entity.class));
  }

  @Test
  public void testAcceptProposalsAlreadyAccepted() throws Exception {
    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ActionRequestService mockActionRequestService = Mockito.mock(ActionRequestService.class);

    AcceptProposalsResolver resolver =
        new AcceptProposalsResolver(mockEntityService, mockActionRequestService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(ImmutableList.of(ACCEPTED_ACTION_REQUEST.toString()));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).join();

    // Basic check to verify that no events went through
    Mockito.verify(mockEntityService, Mockito.times(0))
        .ingestProposal(
            Mockito.any(OperationContext.class),
            Mockito.any(AspectsBatch.class),
            Mockito.anyBoolean());

    // Verify the proposal was not marked
    Mockito.verify(mockActionRequestService, Mockito.times(0))
        .completeProposal(
            Mockito.any(OperationContext.class),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(Entity.class));
  }

  @Test
  public void testAcceptProposalsUnauthorized() throws Exception {
    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ActionRequestService mockActionRequestService = Mockito.mock(ActionRequestService.class);
    AcceptProposalsResolver resolver =
        new AcceptProposalsResolver(mockEntityService, mockActionRequestService);

    // Execute resolver
    QueryContext mockContext = getMockDenyContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(ImmutableList.of(LEGACY_TAG_ACTION_REQUEST.toString()));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    Mockito.verify(mockActionRequestService, Mockito.times(0))
        .completeProposal(any(), any(), anyString(), anyString(), any(), any(Entity.class));
  }

  @Test
  public void testAcceptProposalsServiceException() throws Exception {
    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ActionRequestService mockActionRequestService = Mockito.mock(ActionRequestService.class);
    Mockito.doThrow(RuntimeException.class)
        .when(mockActionRequestService)
        .completeProposal(any(), any(), anyString(), anyString(), any(), any(Entity.class));

    AcceptProposalsResolver resolver =
        new AcceptProposalsResolver(mockEntityService, mockActionRequestService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(ImmutableList.of(LEGACY_TAG_ACTION_REQUEST.toString()));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
  }

  private EntityService<?> initMockEntityService() {
    EntityService<?> mockEntityService = Mockito.mock(EntityService.class);
    Map<Urn, Entity> entityMap = new HashMap<>();

    entityMap.put(
        LEGACY_TAG_ACTION_REQUEST,
        new Entity().setValue(buildLegacyMockTagProposalSnapshot(LEGACY_TAG_ACTION_REQUEST)));

    entityMap.put(
        TAG_ACTION_REQUEST,
        new Entity().setValue(buildMockTagProposalSnapshot(TAG_ACTION_REQUEST)));

    entityMap.put(
        LEGACY_TERM_ACTION_REQUEST,
        new Entity().setValue(buildMockLegacyTermProposalSnapshot(LEGACY_TERM_ACTION_REQUEST)));

    entityMap.put(
        TERM_ACTION_REQUEST,
        new Entity().setValue(buildMockTermProposalSnapshot(TERM_ACTION_REQUEST)));

    entityMap.put(
        DESCRIPTION_ACTION_REQUEST,
        new Entity().setValue(buildMockDocumentationSnapshot(DESCRIPTION_ACTION_REQUEST)));

    entityMap.put(
        STRUCTURED_PROPERTY_ACTION_REQUEST,
        new Entity()
            .setValue(buildMockStructuredPropertySnapshot(STRUCTURED_PROPERTY_ACTION_REQUEST)));

    entityMap.put(
        DOMAIN_ACTION_REQUEST,
        new Entity().setValue(buildMockDomainSnapshot(DOMAIN_ACTION_REQUEST)));

    entityMap.put(
        OWNER_ACTION_REQUEST, new Entity().setValue(buildMockOwnerSnapshot(OWNER_ACTION_REQUEST)));

    entityMap.put(
        ACCEPTED_ACTION_REQUEST,
        new Entity().setValue(buildMockAcceptedSnapshot(ACCEPTED_ACTION_REQUEST)));

    // Individual Lookups
    Mockito.when(
            mockEntityService.getEntities(
                any(), Mockito.eq(ImmutableSet.of(LEGACY_TAG_ACTION_REQUEST)), any(), eq(true)))
        .thenReturn(entityMap);

    Mockito.when(
            mockEntityService.getEntities(
                any(), Mockito.eq(ImmutableSet.of(TAG_ACTION_REQUEST)), any(), eq(true)))
        .thenReturn(entityMap);

    Mockito.when(
            mockEntityService.getEntities(
                any(), Mockito.eq(ImmutableSet.of(LEGACY_TERM_ACTION_REQUEST)), any(), eq(true)))
        .thenReturn(entityMap);

    Mockito.when(
            mockEntityService.getEntities(
                any(), Mockito.eq(ImmutableSet.of(TERM_ACTION_REQUEST)), any(), eq(true)))
        .thenReturn(entityMap);

    Mockito.when(
            mockEntityService.getEntities(
                any(), Mockito.eq(ImmutableSet.of(DESCRIPTION_ACTION_REQUEST)), any(), eq(true)))
        .thenReturn(entityMap);

    Mockito.when(
            mockEntityService.getEntities(
                any(),
                Mockito.eq(ImmutableSet.of(STRUCTURED_PROPERTY_ACTION_REQUEST)),
                any(),
                eq(true)))
        .thenReturn(entityMap);

    Mockito.when(
            mockEntityService.getEntities(
                any(), Mockito.eq(ImmutableSet.of(DOMAIN_ACTION_REQUEST)), any(), eq(true)))
        .thenReturn(entityMap);

    Mockito.when(
            mockEntityService.getEntities(
                any(), Mockito.eq(ImmutableSet.of(OWNER_ACTION_REQUEST)), any(), eq(true)))
        .thenReturn(entityMap);

    Mockito.when(
            mockEntityService.getEntities(
                any(), Mockito.eq(ImmutableSet.of(ACCEPTED_ACTION_REQUEST)), any(), eq(true)))
        .thenReturn(entityMap);

    // Batch Lookups
    Mockito.when(
            mockEntityService.getEntities(
                any(),
                Mockito.eq(ImmutableSet.of(LEGACY_TAG_ACTION_REQUEST, LEGACY_TERM_ACTION_REQUEST)),
                any(),
                eq(true)))
        .thenReturn(entityMap);

    return mockEntityService;
  }
}
