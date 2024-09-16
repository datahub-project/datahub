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
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_RESULT_REJECTED;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_STATUS_COMPLETE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.ProposalService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RejectProposalsResolverTest {

  @Test
  public void testRejectProposalsInvalidUrns() throws Exception {
    Set<String> badProposalsUrn = ImmutableSet.of("urn:li:action:123");

    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ProposalService mockProposalService = Mockito.mock(ProposalService.class);
    RejectProposalsResolver resolver =
        new RejectProposalsResolver(mockEntityService, mockProposalService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urns"))).thenReturn(badProposalsUrn);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Assert.assertThrows(ExecutionException.class, () -> resolver.get(mockEnv).get());
  }

  @Test
  public void testRejectSuccess() throws Exception {
    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ProposalService mockProposalService = Mockito.mock(ProposalService.class);

    RejectProposalsResolver resolver =
        new RejectProposalsResolver(mockEntityService, mockProposalService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(ImmutableList.of(TAG_ACTION_REQUEST.toString()));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).join();

    // Verify the proposal was marked as rejected
    Mockito.verify(mockProposalService, Mockito.times(1))
        .completeProposal(
            Mockito.any(OperationContext.class),
            Mockito.eq(TEST_ACTOR_URN),
            Mockito.eq(ACTION_REQUEST_STATUS_COMPLETE),
            Mockito.eq(ACTION_REQUEST_RESULT_REJECTED),
            Mockito.any(Entity.class));
  }

  @Test
  public void testRejectSuccessMultiple() throws Exception {
    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ProposalService mockProposalService = Mockito.mock(ProposalService.class);

    RejectProposalsResolver resolver =
        new RejectProposalsResolver(mockEntityService, mockProposalService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(
            ImmutableList.of(TAG_ACTION_REQUEST.toString(), TERM_ACTION_REQUEST.toString()));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).join();

    // Verify the proposal was marked as rejected
    Mockito.verify(mockProposalService, Mockito.times(2))
        .completeProposal(
            Mockito.any(OperationContext.class),
            Mockito.eq(TEST_ACTOR_URN),
            Mockito.eq(ACTION_REQUEST_STATUS_COMPLETE),
            Mockito.eq(ACTION_REQUEST_RESULT_REJECTED),
            Mockito.any(Entity.class));
  }

  @Test
  public void testRejectProposalsUnauthorized() {
    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ProposalService mockProposalService = Mockito.mock(ProposalService.class);
    RejectProposalsResolver resolver =
        new RejectProposalsResolver(mockEntityService, mockProposalService);

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
  public void testRejectProposalsServiceException() {
    // Create resolver
    EntityService<?> mockEntityService = initMockEntityService();
    ProposalService mockProposalService = Mockito.mock(ProposalService.class);
    Mockito.doThrow(RuntimeException.class)
        .when(mockProposalService)
        .completeProposal(any(), any(), anyString(), anyString(), any(Entity.class));

    RejectProposalsResolver resolver =
        new RejectProposalsResolver(mockEntityService, mockProposalService);

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
