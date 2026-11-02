package com.datahub.authorization;

import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.UI_SOURCE;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.role.RoleService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RoleServiceTest {
  private static final String ROLE_URN_STRING = "urn:li:dataHubRole:Admin";
  private static final String FIRST_ACTOR_URN_STRING = "urn:li:corpuser:foo";
  private static final String SECOND_ACTOR_URN_STRING = "urn:li:corpuser:bar";
  private static final String DATAHUB_SYSTEM_CLIENT_ID = "__datahub_system";
  private static final Authentication SYSTEM_AUTHENTICATION =
      new Authentication(new Actor(ActorType.USER, DATAHUB_SYSTEM_CLIENT_ID), "");
  private Urn roleUrn;
  private Urn firstActorUrn;
  private Urn secondActorUrn;
  private EntityClient _entityClient;
  private RoleService _roleService;
  private OperationContext opContext =
      TestOperationContexts.userContextNoSearchAuthorization(SYSTEM_AUTHENTICATION);

  @BeforeMethod
  public void setupTest() throws Exception {
    roleUrn = Urn.createFromString(ROLE_URN_STRING);
    firstActorUrn = Urn.createFromString(FIRST_ACTOR_URN_STRING);
    secondActorUrn = Urn.createFromString(SECOND_ACTOR_URN_STRING);
    _entityClient = mock(EntityClient.class);
    when(_entityClient.exists(any(), eq(roleUrn))).thenReturn(true);

    _roleService = new RoleService(_entityClient);
  }

  @Test
  public void testBatchAssignRoleNoActorExists() throws Exception {
    when(_entityClient.filterExistingUrns(any(OperationContext.class), anyCollection()))
        .thenReturn(ImmutableSet.of());

    _roleService.batchAssignRoleToActors(
        opContext, ImmutableList.of(FIRST_ACTOR_URN_STRING), roleUrn);

    // Nothing was assignable, so no ingest should have been attempted at all.
    verify(_entityClient, never()).batchIngestProposals(any(), anyCollection(), anyBoolean());
    verify(_entityClient, never()).ingestProposal(any(OperationContext.class), any(), eq(false));
  }

  @Test
  public void testBatchAssignRoleSomeActorExists() throws Exception {
    when(_entityClient.filterExistingUrns(any(OperationContext.class), anyCollection()))
        .thenReturn(ImmutableSet.of(firstActorUrn));

    _roleService.batchAssignRoleToActors(
        opContext, ImmutableList.of(FIRST_ACTOR_URN_STRING, SECOND_ACTOR_URN_STRING), roleUrn);

    // Only the existing actor is assigned, and the UI source marker is preserved.
    final Collection<MetadataChangeProposal> proposals = captureBatch();
    assertEquals(1, proposals.size());
    final MetadataChangeProposal proposal = proposals.iterator().next();
    assertEquals(firstActorUrn, proposal.getEntityUrn());
    assertEquals(UI_SOURCE, proposal.getSystemMetadata().getProperties().get(APP_SOURCE));
  }

  @Test
  public void testBatchAssignRoleAllActorsExist() throws Exception {
    when(_entityClient.filterExistingUrns(any(OperationContext.class), anyCollection()))
        .thenReturn(ImmutableSet.of(firstActorUrn, secondActorUrn));

    _roleService.batchAssignRoleToActors(
        opContext, ImmutableList.of(FIRST_ACTOR_URN_STRING, SECOND_ACTOR_URN_STRING), roleUrn);

    // Both actors go out in a single request, and existence is resolved in a single query.
    assertEquals(2, captureBatch().size());
    verify(_entityClient, times(1))
        .filterExistingUrns(any(OperationContext.class), anyCollection());
    verify(_entityClient, never()).ingestProposal(any(OperationContext.class), any(), eq(false));
  }

  @Test
  public void testAssignNullRoleToActorAllActorsExist() throws Exception {
    when(_entityClient.filterExistingUrns(any(OperationContext.class), anyCollection()))
        .thenReturn(ImmutableSet.of(firstActorUrn));

    _roleService.batchAssignRoleToActors(opContext, ImmutableList.of(FIRST_ACTOR_URN_STRING), null);

    final Collection<MetadataChangeProposal> proposals = captureBatch();
    assertEquals(1, proposals.size());
    // A null role clears membership rather than assigning one.
    assertTrue(
        proposals
            .iterator()
            .next()
            .getAspect()
            .getValue()
            .asString(StandardCharsets.UTF_8)
            .contains("\"roles\":[]"));
  }

  @Test
  public void testBatchAssignRoleFallsBackToIndividualIngestOnBatchFailure() throws Exception {
    when(_entityClient.filterExistingUrns(any(OperationContext.class), anyCollection()))
        .thenReturn(ImmutableSet.of(firstActorUrn, secondActorUrn));
    when(_entityClient.batchIngestProposals(any(), anyCollection(), eq(false)))
        .thenThrow(new RuntimeException("batch failed"));
    // The first actor is retried successfully, the second keeps failing and is skipped.
    when(_entityClient.ingestProposal(any(OperationContext.class), any(), eq(false)))
        .thenReturn(FIRST_ACTOR_URN_STRING)
        .thenThrow(new RuntimeException("individual failure"));

    _roleService.batchAssignRoleToActors(
        opContext, ImmutableList.of(FIRST_ACTOR_URN_STRING, SECOND_ACTOR_URN_STRING), roleUrn);

    // A failed batch must not abandon the assignments, and one bad actor must not stop the other.
    verify(_entityClient, times(2)).ingestProposal(any(OperationContext.class), any(), eq(false));
  }

  @Test
  public void testBatchAssignRoleSkipsMalformedActorUrn() throws Exception {
    when(_entityClient.filterExistingUrns(any(OperationContext.class), anyCollection()))
        .thenReturn(ImmutableSet.of(firstActorUrn));

    _roleService.batchAssignRoleToActors(
        opContext, ImmutableList.of(FIRST_ACTOR_URN_STRING, "not a urn"), roleUrn);

    // The malformed entry is dropped before the existence query rather than failing the request.
    final Collection<MetadataChangeProposal> proposals = captureBatch();
    assertEquals(1, proposals.size());
    assertEquals(firstActorUrn, proposals.iterator().next().getEntityUrn());
  }

  @Test
  public void testBatchAssignRoleNoActorsMakesNoCalls() throws Exception {
    _roleService.batchAssignRoleToActors(opContext, ImmutableList.of(), roleUrn);

    verify(_entityClient, never()).filterExistingUrns(any(OperationContext.class), anyCollection());
    verify(_entityClient, never()).batchIngestProposals(any(), anyCollection(), anyBoolean());
  }

  @SuppressWarnings("unchecked")
  private Collection<MetadataChangeProposal> captureBatch() throws Exception {
    final ArgumentCaptor<Collection<MetadataChangeProposal>> captor =
        ArgumentCaptor.forClass(Collection.class);
    verify(_entityClient, times(1))
        .batchIngestProposals(any(OperationContext.class), captor.capture(), eq(false));
    return captor.getValue();
  }
}
