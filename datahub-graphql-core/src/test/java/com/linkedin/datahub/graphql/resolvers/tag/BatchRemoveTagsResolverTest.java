package com.linkedin.datahub.graphql.resolvers.tag;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchRemoveTagsInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.generated.SubResourceType;
import com.linkedin.datahub.graphql.resolvers.mutate.BatchRemoveTagsResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaMetadata;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class BatchRemoveTagsResolverTest {

  private static final String TEST_ENTITY_URN_1 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_ENTITY_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-2,PROD)";
  private static final String TEST_TAG_1_URN = "urn:li:tag:test-id-1";
  private static final String TEST_TAG_2_URN = "urn:li:tag:test-id-2";

  @Test
  public void testGetSuccessNoExistingTags() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);
    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
                Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);

    stubExistingUrns(
        mockService,
        Urn.createFromString(TEST_ENTITY_URN_1),
        Urn.createFromString(TEST_ENTITY_URN_2),
        Urn.createFromString(TEST_TAG_1_URN),
        Urn.createFromString(TEST_TAG_2_URN));

    BatchRemoveTagsResolver resolver = new BatchRemoveTagsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveTagsInput input =
        new BatchRemoveTagsInput(
            ImmutableList.of(TEST_TAG_1_URN, TEST_TAG_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    final GlobalTags emptyTags =
        new GlobalTags().setTags(new TagAssociationArray(Collections.emptyList()));

    final MetadataChangeProposal proposal1 =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN_1), GLOBAL_TAGS_ASPECT_NAME, emptyTags);
    final MetadataChangeProposal proposal2 =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN_2), GLOBAL_TAGS_ASPECT_NAME, emptyTags);
    proposal2.setEntityUrn(Urn.createFromString(TEST_ENTITY_URN_2));
    proposal2.setEntityType(Constants.DATASET_ENTITY_NAME);
    proposal2.setAspectName(Constants.GLOBAL_TAGS_ASPECT_NAME);
    proposal2.setAspect(GenericRecordUtils.serializeAspect(emptyTags));
    proposal2.setChangeType(ChangeType.UPSERT);

    verifyIngestProposal(mockService, 1, List.of(proposal1, proposal2));

    verifyExistenceResolvedInBatches(mockService, 1);
  }

  @Test
  public void testGetSuccessExistingTags() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    final GlobalTags oldTags1 =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_1_URN)),
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_2_URN)))));

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldTags1);

    final GlobalTags oldTags2 =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_1_URN)))));

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
                Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldTags2);

    stubExistingUrns(
        mockService,
        Urn.createFromString(TEST_ENTITY_URN_1),
        Urn.createFromString(TEST_ENTITY_URN_2),
        Urn.createFromString(TEST_TAG_1_URN),
        Urn.createFromString(TEST_TAG_2_URN));

    BatchRemoveTagsResolver resolver = new BatchRemoveTagsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveTagsInput input =
        new BatchRemoveTagsInput(
            ImmutableList.of(TEST_TAG_1_URN, TEST_TAG_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    final GlobalTags emptyTags =
        new GlobalTags().setTags(new TagAssociationArray(Collections.emptyList()));

    final MetadataChangeProposal proposal1 =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN_1), GLOBAL_TAGS_ASPECT_NAME, emptyTags);
    final MetadataChangeProposal proposal2 =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN_2), GLOBAL_TAGS_ASPECT_NAME, emptyTags);

    verifyIngestProposal(mockService, 1, List.of(proposal1, proposal2));

    verifyExistenceResolvedInBatches(mockService, 1);
  }

  @Test
  public void testGetSuccessTwoColumnsOnOneDataset() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    Urn datasetUrn = UrnUtils.getUrn(TEST_ENTITY_URN_1);
    GlobalTags tag1 =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_1_URN)))));

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(datasetUrn),
                Mockito.eq(SCHEMA_METADATA_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(
            new SchemaMetadata()
                .setFields(
                    new SchemaFieldArray(
                        ImmutableList.of(
                            new SchemaField().setFieldPath("col_a"),
                            new SchemaField().setFieldPath("col_b")))));
    EditableSchemaMetadata stored =
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(
                new EditableSchemaFieldInfoArray(
                    ImmutableList.of(
                        new EditableSchemaFieldInfo()
                            .setFieldPath("col_a")
                            .setGlobalTags(tag1.copy()),
                        new EditableSchemaFieldInfo()
                            .setFieldPath("col_b")
                            .setGlobalTags(tag1.copy()))));
    // Return a fresh copy per read, like the real store. A shared instance would collect
    // every column's edit and hide the lost-update bug.
    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(datasetUrn),
                Mockito.eq(EDITABLE_SCHEMA_METADATA_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenAnswer(invocation -> stored.copy());

    stubExistingUrns(mockService, datasetUrn, Urn.createFromString(TEST_TAG_1_URN));

    BatchRemoveTagsResolver resolver = new BatchRemoveTagsResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveTagsInput input =
        new BatchRemoveTagsInput(
            ImmutableList.of(TEST_TAG_1_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "col_a"),
                new ResourceRefInput(TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "col_b")));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    ArgumentCaptor<AspectsBatchImpl> batchCaptor = ArgumentCaptor.forClass(AspectsBatchImpl.class);
    Mockito.verify(mockService, Mockito.times(1))
        .ingestProposal(any(), batchCaptor.capture(), Mockito.eq(false));

    // Both columns must land in a single write. Two writes of the whole aspect would each
    // clear one column, and the second would restore the tag on the first.
    List<BatchItem> editableSchemaWrites =
        batchCaptor.getValue().getItems().stream()
            .filter(item -> item.getAspectName().equals(EDITABLE_SCHEMA_METADATA_ASPECT_NAME))
            .map(item -> (BatchItem) item)
            .toList();
    assertEquals(editableSchemaWrites.size(), 1);
    EditableSchemaMetadata written =
        editableSchemaWrites.get(0).getAspect(EditableSchemaMetadata.class);
    assertEquals(written.getEditableSchemaFieldInfo().size(), 2);
    for (EditableSchemaFieldInfo fieldInfo : written.getEditableSchemaFieldInfo()) {
      assertTrue(fieldInfo.getGlobalTags().getTags().isEmpty());
    }
  }

  @Test
  public void testGetFailureResourceDoesNotExist() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);
    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
                Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);

    // The first resource does not exist.
    stubExistingUrns(
        mockService, Urn.createFromString(TEST_ENTITY_URN_2), Urn.createFromString(TEST_TAG_1_URN));

    BatchRemoveTagsResolver resolver = new BatchRemoveTagsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveTagsInput input =
        new BatchRemoveTagsInput(
            ImmutableList.of(TEST_TAG_1_URN, TEST_TAG_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0))
        .ingestProposal(any(), Mockito.any(AspectsBatchImpl.class), Mockito.anyBoolean());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    BatchRemoveTagsResolver resolver = new BatchRemoveTagsResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveTagsInput input =
        new BatchRemoveTagsInput(
            ImmutableList.of(TEST_TAG_1_URN, TEST_TAG_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0))
        .ingestProposal(any(), Mockito.any(AspectsBatchImpl.class), Mockito.anyBoolean());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .ingestProposal(any(), Mockito.any(AspectsBatchImpl.class), Mockito.anyBoolean());

    BatchRemoveTagsResolver resolver = new BatchRemoveTagsResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    BatchRemoveTagsInput input =
        new BatchRemoveTagsInput(
            ImmutableList.of(TEST_TAG_1_URN, TEST_TAG_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
