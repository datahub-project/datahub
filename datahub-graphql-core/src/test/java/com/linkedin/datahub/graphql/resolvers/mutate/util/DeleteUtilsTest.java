package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockEntityService;
import static com.linkedin.datahub.graphql.TestUtils.verifyIngestProposal;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class DeleteUtilsTest {

  private static final String URN_1 = "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String URN_2 = "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-2,PROD)";

  @Test
  public void updateStatusForResources_batchesStatusReads() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    QueryContext mockContext = getMockAllowContext();
    Urn urn1 = UrnUtils.getUrn(URN_1);
    Urn urn2 = UrnUtils.getUrn(URN_2);

    Status existing = new Status().setRemoved(false);
    when(mockService.getLatestAspects(
            any(), eq(Set.of(urn1, urn2)), eq(Set.of(Constants.STATUS_ASPECT_NAME)), eq(false)))
        .thenReturn(Map.of(urn1, List.<RecordTemplate>of(existing), urn2, List.of()));

    DeleteUtils.updateStatusForResources(
        mockContext.getOperationContext(),
        true,
        List.of(URN_1, URN_2),
        UrnUtils.getUrn(mockContext.getActorUrn()),
        mockService);

    verify(mockService)
        .getLatestAspects(
            any(), eq(Set.of(urn1, urn2)), eq(Set.of(Constants.STATUS_ASPECT_NAME)), eq(false));
    verify(mockService, never()).getAspect(any(), any(), any(), any(Long.class));

    Status expected = new Status().setRemoved(true);
    MetadataChangeProposal proposal1 =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            urn1, Constants.STATUS_ASPECT_NAME, expected);
    MetadataChangeProposal proposal2 =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            urn2, Constants.STATUS_ASPECT_NAME, expected);
    verifyIngestProposal(mockService, 1, List.of(proposal1, proposal2));
  }

  @Test
  public void updateStatusForResources_undeleteExistingStatus() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    QueryContext mockContext = getMockAllowContext();
    Urn urn1 = UrnUtils.getUrn(URN_1);

    Status existing = new Status().setRemoved(true);
    when(mockService.getLatestAspects(
            any(), eq(Set.of(urn1)), eq(Set.of(Constants.STATUS_ASPECT_NAME)), eq(false)))
        .thenReturn(Map.of(urn1, List.<RecordTemplate>of(existing)));

    DeleteUtils.updateStatusForResources(
        mockContext.getOperationContext(),
        false,
        List.of(URN_1),
        UrnUtils.getUrn(mockContext.getActorUrn()),
        mockService);

    Status expected = new Status().setRemoved(false);
    MetadataChangeProposal proposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            urn1, Constants.STATUS_ASPECT_NAME, expected);
    verifyIngestProposal(mockService, 1, List.of(proposal));
  }

  @Test
  public void updateStatusForResources_doesNotMutateCachedStatus() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    QueryContext mockContext = getMockAllowContext();
    Urn urn1 = UrnUtils.getUrn(URN_1);

    Status existing = new Status().setRemoved(false);
    when(mockService.getLatestAspects(
            any(), eq(Set.of(urn1)), eq(Set.of(Constants.STATUS_ASPECT_NAME)), eq(false)))
        .thenReturn(Map.of(urn1, List.<RecordTemplate>of(existing)));

    DeleteUtils.updateStatusForResources(
        mockContext.getOperationContext(),
        true,
        List.of(URN_1),
        UrnUtils.getUrn(mockContext.getActorUrn()),
        mockService);

    // copyStatus must not mutate the aspect instance returned by the batch read
    assertFalse(existing.isRemoved());
  }

  @Test
  public void updateStatusForResources_dedupesDuplicateUrns() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    QueryContext mockContext = getMockAllowContext();
    Urn urn1 = UrnUtils.getUrn(URN_1);

    when(mockService.getLatestAspects(
            any(), eq(Set.of(urn1)), eq(Set.of(Constants.STATUS_ASPECT_NAME)), eq(false)))
        .thenReturn(Map.of());

    DeleteUtils.updateStatusForResources(
        mockContext.getOperationContext(),
        true,
        List.of(URN_1, URN_1, URN_1),
        UrnUtils.getUrn(mockContext.getActorUrn()),
        mockService);

    verify(mockService)
        .getLatestAspects(
            any(), eq(Set.of(urn1)), eq(Set.of(Constants.STATUS_ASPECT_NAME)), eq(false));

    Status expected = new Status().setRemoved(true);
    MetadataChangeProposal proposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            urn1, Constants.STATUS_ASPECT_NAME, expected);
    // One MCP only — not three for the three duplicate input strings.
    verifyIngestProposal(mockService, 1, List.of(proposal));
  }

  @Test
  public void updateStatusForResources_emptyListNoOps() {
    EntityService<?> mockService = getMockEntityService();
    QueryContext mockContext = getMockAllowContext();
    DeleteUtils.updateStatusForResources(
        mockContext.getOperationContext(),
        true,
        List.of(),
        UrnUtils.getUrn(mockContext.getActorUrn()),
        mockService);
    verify(mockService, never()).getLatestAspects(any(), any(), any(), any(Boolean.class));
    verify(mockService, never()).ingestProposal(any(), any(), any(Boolean.class));
  }
}
