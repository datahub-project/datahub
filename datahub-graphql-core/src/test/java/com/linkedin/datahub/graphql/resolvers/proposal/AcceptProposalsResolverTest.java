package com.linkedin.datahub.graphql.resolvers.proposal;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.ACCEPTED_ACTION_REQUEST;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.DESCRIPTION_ACTION_REQUEST;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.TAG_ACTION_REQUEST;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.TERM_ACTION_REQUEST;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.TEST_ACTOR_URN;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.buildMockAcceptedSnapshot;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.buildMockDocumentationSnapshot;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.buildMockTagProposalSnapshot;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalTestUtils.buildMockTermProposalSnapshot;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_RESULT_ACCEPTED;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_STATUS_COMPLETE;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.ProposalService;
import com.linkedin.metadata.snapshot.ActionRequestSnapshot;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AcceptProposalsResolverTest {

  @Test
  public void testAcceptProposalsInvalidUrns() throws Exception {
    Set<String> badProposalsUrn = ImmutableSet.of("urn:li:action:123");

    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ProposalService mockProposalService = Mockito.mock(ProposalService.class);
    AcceptProposalsResolver resolver =
        new AcceptProposalsResolver(mockEntityService, mockProposalService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urns"))).thenReturn(badProposalsUrn);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Assert.assertThrows(ExecutionException.class, () -> resolver.get(mockEnv).get());
  }

  @Test
  public void testAcceptProposalsSuccessTags() throws Exception {
    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ProposalService mockProposalService = Mockito.mock(ProposalService.class);

    AcceptProposalsResolver resolver =
        new AcceptProposalsResolver(mockEntityService, mockProposalService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(ImmutableList.of(TAG_ACTION_REQUEST.toString()));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).join();

    // Basic check to verify that the tag change went through.
    Mockito.verify(mockEntityService, Mockito.times(1))
        .ingestProposal(
            Mockito.any(OperationContext.class),
            Mockito.any(AspectsBatch.class),
            Mockito.eq(false));

    // Verify the proposal was marked as completed
    Mockito.verify(mockProposalService, Mockito.times(1))
        .completeProposal(
            Mockito.any(OperationContext.class),
            Mockito.eq(TEST_ACTOR_URN),
            Mockito.eq(ACTION_REQUEST_STATUS_COMPLETE),
            Mockito.eq(ACTION_REQUEST_RESULT_ACCEPTED),
            Mockito.any(Entity.class));
  }

  @Test
  public void testAcceptProposalsSuccessTerms() throws Exception {
    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ProposalService mockProposalService = Mockito.mock(ProposalService.class);

    AcceptProposalsResolver resolver =
        new AcceptProposalsResolver(mockEntityService, mockProposalService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(ImmutableList.of(TERM_ACTION_REQUEST.toString()));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).join();

    // Basic check to verify that the tag change went through.
    Mockito.verify(mockEntityService, Mockito.times(1))
        .ingestProposal(
            Mockito.any(OperationContext.class),
            Mockito.any(AspectsBatch.class),
            Mockito.eq(false));

    // Verify the proposal was marked as completed
    Mockito.verify(mockProposalService, Mockito.times(1))
        .completeProposal(
            Mockito.any(OperationContext.class),
            Mockito.eq(TEST_ACTOR_URN),
            Mockito.eq(ACTION_REQUEST_STATUS_COMPLETE),
            Mockito.eq(ACTION_REQUEST_RESULT_ACCEPTED),
            Mockito.any(Entity.class));
  }

  @Test
  public void testAcceptProposalsSuccessUpdateDescription() throws Exception {
    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ProposalService mockProposalService = Mockito.mock(ProposalService.class);

    AcceptProposalsResolver resolver =
        new AcceptProposalsResolver(mockEntityService, mockProposalService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(ImmutableList.of(DESCRIPTION_ACTION_REQUEST.toString()));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).join();

    // Basic check to verify that the tag change went through.
    Mockito.verify(mockProposalService, Mockito.times(1))
        .acceptUpdateResourceDescriptionProposal(
            Mockito.any(OperationContext.class), Mockito.any(ActionRequestSnapshot.class));

    // Verify the proposal was marked as completed
    Mockito.verify(mockProposalService, Mockito.times(1))
        .completeProposal(
            Mockito.any(OperationContext.class),
            Mockito.eq(TEST_ACTOR_URN),
            Mockito.eq(ACTION_REQUEST_STATUS_COMPLETE),
            Mockito.eq(ACTION_REQUEST_RESULT_ACCEPTED),
            Mockito.any(Entity.class));
  }

  @Test
  public void testAcceptProposalsSuccessMultiple() throws Exception {
    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ProposalService mockProposalService = Mockito.mock(ProposalService.class);

    AcceptProposalsResolver resolver =
        new AcceptProposalsResolver(mockEntityService, mockProposalService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(
            ImmutableList.of(TAG_ACTION_REQUEST.toString(), TERM_ACTION_REQUEST.toString()));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).join();

    // Basic check to verify that the tag change went through.
    Mockito.verify(mockEntityService, Mockito.times(2))
        .ingestProposal(
            Mockito.any(OperationContext.class),
            Mockito.any(AspectsBatch.class),
            Mockito.eq(false));

    // Verify the proposal was marked as completed
    Mockito.verify(mockProposalService, Mockito.times(2))
        .completeProposal(
            Mockito.any(OperationContext.class),
            Mockito.eq(TEST_ACTOR_URN),
            Mockito.eq(ACTION_REQUEST_STATUS_COMPLETE),
            Mockito.eq(ACTION_REQUEST_RESULT_ACCEPTED),
            Mockito.any(Entity.class));
  }

  @Test
  public void testAcceptProposalsAlreadyAccepted() throws Exception {
    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ProposalService mockProposalService = Mockito.mock(ProposalService.class);

    AcceptProposalsResolver resolver =
        new AcceptProposalsResolver(mockEntityService, mockProposalService);

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
    Mockito.verify(mockProposalService, Mockito.times(0))
        .completeProposal(
            Mockito.any(OperationContext.class),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(Entity.class));
  }

  @Test
  public void testAcceptProposalsUnauthorized() throws Exception {
    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ProposalService mockProposalService = Mockito.mock(ProposalService.class);
    AcceptProposalsResolver resolver =
        new AcceptProposalsResolver(mockEntityService, mockProposalService);

    // Execute resolver
    QueryContext mockContext = getMockDenyContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(ImmutableList.of(TAG_ACTION_REQUEST.toString()));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    Mockito.verify(mockProposalService, Mockito.times(0))
        .completeProposal(any(), any(), anyString(), anyString(), any(Entity.class));
  }

  @Test
  public void testAcceptProposalsServiceException() throws Exception {
    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ProposalService mockProposalService = Mockito.mock(ProposalService.class);
    Mockito.doThrow(RuntimeException.class)
        .when(mockProposalService)
        .completeProposal(any(), any(), anyString(), anyString(), any(Entity.class));

    AcceptProposalsResolver resolver =
        new AcceptProposalsResolver(mockEntityService, mockProposalService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(ImmutableList.of(TAG_ACTION_REQUEST.toString()));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
  }

  private EntityService<?> initMockEntityService() {
    EntityService<?> mockEntityService = Mockito.mock(EntityService.class);
    Map<Urn, Entity> entityMap = new HashMap<>();

    entityMap.put(
        TAG_ACTION_REQUEST,
        new Entity().setValue(buildMockTagProposalSnapshot(TAG_ACTION_REQUEST)));

    entityMap.put(
        TERM_ACTION_REQUEST,
        new Entity().setValue(buildMockTermProposalSnapshot(TERM_ACTION_REQUEST)));

    entityMap.put(
        DESCRIPTION_ACTION_REQUEST,
        new Entity().setValue(buildMockDocumentationSnapshot(DESCRIPTION_ACTION_REQUEST)));

    entityMap.put(
        ACCEPTED_ACTION_REQUEST,
        new Entity().setValue(buildMockAcceptedSnapshot(ACCEPTED_ACTION_REQUEST)));

    // Individual Lookups
    Mockito.when(
            mockEntityService.getEntities(
                any(), Mockito.eq(ImmutableSet.of(TAG_ACTION_REQUEST)), any(), eq(true)))
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
                any(), Mockito.eq(ImmutableSet.of(ACCEPTED_ACTION_REQUEST)), any(), eq(true)))
        .thenReturn(entityMap);

    // Batch Lookups
    Mockito.when(
            mockEntityService.getEntities(
                any(),
                Mockito.eq(ImmutableSet.of(TAG_ACTION_REQUEST, TERM_ACTION_REQUEST)),
                any(),
                eq(true)))
        .thenReturn(entityMap);

    return mockEntityService;
  }
}
