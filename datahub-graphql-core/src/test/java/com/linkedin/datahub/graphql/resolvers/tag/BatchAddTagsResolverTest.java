package com.linkedin.datahub.graphql.resolvers.tag;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchAddTagsInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.resolvers.mutate.BatchAddTagsResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.transactions.AspectsBatchImpl;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class BatchAddTagsResolverTest {

  private static final String TEST_ENTITY_URN_1 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_ENTITY_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-2,PROD)";
  private static final String TEST_TAG_1_URN = "urn:li:tag:test-id-1";
  private static final String TEST_TAG_2_URN = "urn:li:tag:test-id-2";

  @Test
  public void testGetSuccessNoExistingTags() throws Exception {
    EntityService mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(
            mockService.getAspect(
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
                Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_1))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_2))).thenReturn(true);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_TAG_1_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TAG_2_URN))).thenReturn(true);

    BatchAddTagsResolver resolver = new BatchAddTagsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddTagsInput input =
        new BatchAddTagsInput(
            ImmutableList.of(TEST_TAG_1_URN, TEST_TAG_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    final GlobalTags newTags =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_1_URN)),
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_2_URN)))));

    final MetadataChangeProposal proposal1 =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN_1), GLOBAL_TAGS_ASPECT_NAME, newTags);
    final MetadataChangeProposal proposal2 =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN_2), GLOBAL_TAGS_ASPECT_NAME, newTags);

    verifyIngestProposal(mockService, 1, List.of(proposal1, proposal2));

    Mockito.verify(mockService, Mockito.times(1))
        .exists(Mockito.eq(Urn.createFromString(TEST_TAG_1_URN)));

    Mockito.verify(mockService, Mockito.times(1))
        .exists(Mockito.eq(Urn.createFromString(TEST_TAG_2_URN)));
  }

  @Test
  public void testGetSuccessExistingTags() throws Exception {
    GlobalTags originalTags =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_1_URN)))));

    EntityService mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(originalTags);

    Mockito.when(
            mockService.getAspect(
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
                Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(originalTags);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_1))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_2))).thenReturn(true);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_TAG_1_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TAG_2_URN))).thenReturn(true);

    BatchAddTagsResolver resolver = new BatchAddTagsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddTagsInput input =
        new BatchAddTagsInput(
            ImmutableList.of(TEST_TAG_1_URN, TEST_TAG_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    final GlobalTags newTags =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_1_URN)),
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_2_URN)))));

    final MetadataChangeProposal proposal1 =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN_1), GLOBAL_TAGS_ASPECT_NAME, newTags);
    final MetadataChangeProposal proposal2 =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN_2), GLOBAL_TAGS_ASPECT_NAME, newTags);

    verifyIngestProposal(mockService, 1, List.of(proposal1, proposal2));

    Mockito.verify(mockService, Mockito.times(1))
        .exists(Mockito.eq(Urn.createFromString(TEST_TAG_1_URN)));

    Mockito.verify(mockService, Mockito.times(1))
        .exists(Mockito.eq(Urn.createFromString(TEST_TAG_2_URN)));
  }

  @Test
  public void testGetFailureTagDoesNotExist() throws Exception {
    EntityService mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_1))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TAG_1_URN))).thenReturn(false);

    BatchAddTagsResolver resolver = new BatchAddTagsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddTagsInput input =
        new BatchAddTagsInput(
            ImmutableList.of(TEST_TAG_1_URN, TEST_TAG_2_URN),
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0))
        .ingestProposal(
            Mockito.any(AspectsBatchImpl.class),
            Mockito.any(AuditStamp.class),
            Mockito.anyBoolean());
  }

  @Test
  public void testGetFailureResourceDoesNotExist() throws Exception {
    EntityService mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);
    Mockito.when(
            mockService.getAspect(
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
                Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_1))).thenReturn(false);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_2))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TAG_1_URN))).thenReturn(true);

    BatchAddTagsResolver resolver = new BatchAddTagsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddTagsInput input =
        new BatchAddTagsInput(
            ImmutableList.of(TEST_TAG_1_URN, TEST_TAG_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0))
        .ingestProposal(
            Mockito.any(AspectsBatchImpl.class),
            Mockito.any(AuditStamp.class),
            Mockito.anyBoolean());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    EntityService mockService = getMockEntityService();

    BatchAddTagsResolver resolver = new BatchAddTagsResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddTagsInput input =
        new BatchAddTagsInput(
            ImmutableList.of(TEST_TAG_1_URN, TEST_TAG_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0))
        .ingestProposal(
            Mockito.any(AspectsBatchImpl.class),
            Mockito.any(AuditStamp.class),
            Mockito.anyBoolean());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityService mockService = getMockEntityService();

    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .ingestProposal(
            Mockito.any(AspectsBatchImpl.class),
            Mockito.any(AuditStamp.class),
            Mockito.anyBoolean());

    BatchAddTagsResolver resolver = new BatchAddTagsResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    BatchAddTagsInput input =
        new BatchAddTagsInput(
            ImmutableList.of(TEST_TAG_1_URN),
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
