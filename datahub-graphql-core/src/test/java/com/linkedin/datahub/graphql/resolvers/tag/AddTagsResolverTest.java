package com.linkedin.datahub.graphql.resolvers.tag;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AddTagsInput;
import com.linkedin.datahub.graphql.resolvers.mutate.AddTagsResolver;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;


public class AddTagsResolverTest {

  private static final String TEST_ENTITY_URN = "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_TAG_1_URN = "urn:li:tag:test-id-1";
  private static final String TEST_TAG_2_URN = "urn:li:tag:test-id-2";

  @Test
  public void testGetSuccessNoExistingTags() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);

    Mockito.when(mockService.getAspect(
        Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
        Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
        Mockito.eq(0L)))
      .thenReturn(null);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TAG_1_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TAG_2_URN))).thenReturn(true);

    AddTagsResolver resolver = new AddTagsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    AddTagsInput input = new AddTagsInput(ImmutableList.of(
        TEST_TAG_1_URN,
        TEST_TAG_2_URN
    ), TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    final GlobalTags newTags = new GlobalTags().setTags(new TagAssociationArray(ImmutableList.of(
        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_1_URN)),
        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_2_URN))
    )));

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(Urn.createFromString(TEST_ENTITY_URN));
    proposal.setEntityType(Constants.DATASET_ENTITY_NAME);
    proposal.setAspectName(Constants.GLOBAL_TAGS_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(newTags));
    proposal.setChangeType(ChangeType.UPSERT);

    verifyIngestProposal(mockService, 1, proposal);

    Mockito.verify(mockService, Mockito.times(1)).exists(
        Mockito.eq(Urn.createFromString(TEST_TAG_1_URN))
    );

    Mockito.verify(mockService, Mockito.times(1)).exists(
        Mockito.eq(Urn.createFromString(TEST_TAG_2_URN))
    );
  }

  @Test
  public void testGetSuccessExistingTags() throws Exception {
    GlobalTags originalTags = new GlobalTags().setTags(new TagAssociationArray(ImmutableList.of(
        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_1_URN))))
    );

    EntityService mockService = Mockito.mock(EntityService.class);

    Mockito.when(mockService.getAspect(
        Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
        Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
        Mockito.eq(0L)))
      .thenReturn(originalTags);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TAG_1_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TAG_2_URN))).thenReturn(true);

    AddTagsResolver resolver = new AddTagsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    AddTagsInput input = new AddTagsInput(ImmutableList.of(
        TEST_TAG_1_URN,
        TEST_TAG_2_URN
    ), TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    final GlobalTags newTags = new GlobalTags().setTags(new TagAssociationArray(ImmutableList.of(
        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_1_URN)),
        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_2_URN))
    )));

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(Urn.createFromString(TEST_ENTITY_URN));
    proposal.setEntityType(Constants.DATASET_ENTITY_NAME);
    proposal.setAspectName(Constants.GLOBAL_TAGS_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(newTags));
    proposal.setChangeType(ChangeType.UPSERT);

    verifyIngestProposal(mockService, 1, proposal);

    Mockito.verify(mockService, Mockito.times(1)).exists(
        Mockito.eq(Urn.createFromString(TEST_TAG_1_URN))
    );

    Mockito.verify(mockService, Mockito.times(1)).exists(
        Mockito.eq(Urn.createFromString(TEST_TAG_2_URN))
    );
  }

  @Test
  public void testGetFailureTagDoesNotExist() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);

    Mockito.when(mockService.getAspect(
        Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
        Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
        Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TAG_1_URN))).thenReturn(false);

    AddTagsResolver resolver = new AddTagsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    AddTagsInput input = new AddTagsInput(ImmutableList.of(
        TEST_TAG_1_URN
    ), TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetFailureResourceDoesNotExist() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);

    Mockito.when(mockService.getAspect(
        Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
        Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
        Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(false);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TAG_1_URN))).thenReturn(true);

    AddTagsResolver resolver = new AddTagsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    AddTagsInput input = new AddTagsInput(ImmutableList.of(
        TEST_TAG_1_URN
    ), TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);

    AddTagsResolver resolver = new AddTagsResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    AddTagsInput input = new AddTagsInput(ImmutableList.of(
        TEST_TAG_1_URN
    ), TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);

    Mockito.doThrow(RuntimeException.class).when(mockService).ingestProposal(
        Mockito.any(),
        Mockito.any(AuditStamp.class), Mockito.eq(false));

    AddTagsResolver resolver = new AddTagsResolver(Mockito.mock(EntityService.class));

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    AddTagsInput input = new AddTagsInput(ImmutableList.of(
        TEST_TAG_1_URN
    ), TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}