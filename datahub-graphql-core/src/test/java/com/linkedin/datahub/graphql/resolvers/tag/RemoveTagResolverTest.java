package com.linkedin.datahub.graphql.resolvers.tag;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.SubResourceType;
import com.linkedin.datahub.graphql.generated.TagAssociationInput;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.RemoveTagResolver;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.utils.SchemaFieldUtils;
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
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class RemoveTagResolverTest {

  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_FIELD_PATH = "field0";
  private static final Urn TEST_SCHEMA_FIELD_URN =
      SchemaFieldUtils.generateSchemaFieldUrn(UrnUtils.getUrn(TEST_ENTITY_URN), TEST_FIELD_PATH);

  private static final String TEST_TAG_1_URN = "urn:li:tag:test-id-1";
  private static final String TEST_TAG_2_URN = "urn:li:tag:test-id-2";

  @Test
  public void testGetSuccessNoExistingTags() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TAG_1_URN)), eq(true)))
        .thenReturn(true);

    RemoveTagResolver resolver = new RemoveTagResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    TagAssociationInput input =
        new TagAssociationInput(TEST_TAG_1_URN, TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    final GlobalTags emptyTags =
        new GlobalTags().setTags(new TagAssociationArray(Collections.emptyList()));

    final MetadataChangeProposal proposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN), GLOBAL_TAGS_ASPECT_NAME, emptyTags);

    verifyIngestProposal(mockService, 1, List.of(proposal));
  }

  @Test
  public void testSuccessNoOp() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    final GlobalTags oldTags =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_2_URN)))));

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldTags);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TAG_1_URN)), eq(true)))
        .thenReturn(true);

    RemoveTagResolver resolver = new RemoveTagResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    TagAssociationInput input =
        new TagAssociationInput(TEST_TAG_1_URN, TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    final MetadataChangeProposal proposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN), GLOBAL_TAGS_ASPECT_NAME, oldTags);

    verifyIngestProposal(mockService, 1, List.of(proposal));
  }

  @Test
  public void testGetSuccessExistingTags() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    final GlobalTags oldTags =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_1_URN)),
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_2_URN)))));

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldTags);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TAG_1_URN)), eq(true)))
        .thenReturn(true);

    RemoveTagResolver resolver = new RemoveTagResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    TagAssociationInput input =
        new TagAssociationInput(TEST_TAG_1_URN, TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    final GlobalTags updatedTags =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_2_URN)))));

    final MetadataChangeProposal proposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN), GLOBAL_TAGS_ASPECT_NAME, updatedTags);

    verifyIngestProposal(mockService, 1, List.of(proposal));
  }

  @Test
  public void testGetSuccessSchemaField() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    final SchemaMetadata oldSchema =
        new SchemaMetadata()
            .setFields(
                new SchemaFieldArray(
                    ImmutableList.of(
                        new SchemaField().setFieldPath(TEST_FIELD_PATH),
                        new SchemaField().setFieldPath("field1"))));

    final EditableSchemaMetadata oldEditableSchema =
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(
                new EditableSchemaFieldInfoArray(
                    ImmutableList.of(
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH)
                            .setGlobalTags(
                                new GlobalTags()
                                    .setTags(
                                        new TagAssociationArray(
                                            ImmutableList.of(
                                                new TagAssociation()
                                                    .setTag(
                                                        TagUrn.createFromString(TEST_TAG_1_URN)),
                                                new TagAssociation()
                                                    .setTag(
                                                        TagUrn.createFromString(
                                                            TEST_TAG_2_URN)))))),
                        new EditableSchemaFieldInfo()
                            .setFieldPath("field1")
                            .setGlobalTags(
                                new GlobalTags()
                                    .setTags(
                                        new TagAssociationArray(
                                            ImmutableList.of(
                                                new TagAssociation()
                                                    .setTag(
                                                        TagUrn.createFromString(TEST_TAG_1_URN)),
                                                new TagAssociation()
                                                    .setTag(
                                                        TagUrn.createFromString(
                                                            TEST_TAG_2_URN)))))))));

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(SCHEMA_METADATA_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldSchema);
    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(EDITABLE_SCHEMA_METADATA_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldEditableSchema);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TAG_1_URN)), eq(true)))
        .thenReturn(true);

    RemoveTagResolver resolver = new RemoveTagResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    TagAssociationInput input =
        new TagAssociationInput(
            TEST_TAG_1_URN, TEST_ENTITY_URN, SubResourceType.DATASET_FIELD, TEST_FIELD_PATH);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    final EditableSchemaMetadata newEditableSchema =
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(
                new EditableSchemaFieldInfoArray(
                    ImmutableList.of(
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH)
                            .setGlobalTags(
                                new GlobalTags()
                                    .setTags(
                                        new TagAssociationArray(
                                            ImmutableList.of(
                                                new TagAssociation()
                                                    .setTag(
                                                        TagUrn.createFromString(
                                                            TEST_TAG_2_URN)))))),
                        new EditableSchemaFieldInfo()
                            .setFieldPath("field1")
                            .setGlobalTags(
                                new GlobalTags()
                                    .setTags(
                                        new TagAssociationArray(
                                            ImmutableList.of(
                                                new TagAssociation()
                                                    .setTag(
                                                        TagUrn.createFromString(TEST_TAG_1_URN)),
                                                new TagAssociation()
                                                    .setTag(
                                                        TagUrn.createFromString(
                                                            TEST_TAG_2_URN)))))))));

    final MetadataChangeProposal editableProposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN),
            EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
            newEditableSchema);

    verifyIngestProposal(mockService, 1, List.of(editableProposal));
  }

  @Test
  public void testGetSuccessSchemaFieldNoOp() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    final SchemaMetadata oldSchema =
        new SchemaMetadata()
            .setFields(new SchemaFieldArray(new SchemaField().setFieldPath(TEST_FIELD_PATH)));

    final EditableSchemaMetadata oldEditableSchema =
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(
                new EditableSchemaFieldInfoArray(
                    ImmutableList.of(new EditableSchemaFieldInfo().setFieldPath(TEST_FIELD_PATH))));

    final GlobalTags oldTags =
        new GlobalTags().setTags(new TagAssociationArray(ImmutableList.of()));

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(SCHEMA_METADATA_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldSchema);
    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(EDITABLE_SCHEMA_METADATA_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldEditableSchema);
    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldTags);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TAG_1_URN)), eq(true)))
        .thenReturn(true);

    RemoveTagResolver resolver = new RemoveTagResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    TagAssociationInput input =
        new TagAssociationInput(
            TEST_TAG_1_URN, TEST_ENTITY_URN, SubResourceType.DATASET_FIELD, TEST_FIELD_PATH);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    final EditableSchemaMetadata newEditableSchema =
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(
                new EditableSchemaFieldInfoArray(
                    ImmutableList.of(
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH)
                            .setGlobalTags(
                                new GlobalTags()
                                    .setTags(new TagAssociationArray(ImmutableList.of()))))));
    final GlobalTags updatedTags =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_2_URN)))));

    final MetadataChangeProposal globalTagsProposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN), GLOBAL_TAGS_ASPECT_NAME, updatedTags);
    final MetadataChangeProposal editableProposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN),
            EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
            newEditableSchema);

    verifyIngestProposal(mockService, 1, List.of(editableProposal));
  }

  @Test
  public void testGetSuccessSchemaFieldWithGlobalTagsAspect() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    final SchemaMetadata oldSchema =
        new SchemaMetadata()
            .setFields(
                new SchemaFieldArray(
                    ImmutableList.of(
                        new SchemaField().setFieldPath(TEST_FIELD_PATH),
                        new SchemaField().setFieldPath("field1"))));

    final EditableSchemaMetadata oldEditableSchema =
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(
                new EditableSchemaFieldInfoArray(
                    ImmutableList.of(
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH)
                            .setGlobalTags(
                                new GlobalTags()
                                    .setTags(
                                        new TagAssociationArray(
                                            ImmutableList.of(
                                                new TagAssociation()
                                                    .setTag(
                                                        TagUrn.createFromString(TEST_TAG_1_URN)),
                                                new TagAssociation()
                                                    .setTag(
                                                        TagUrn.createFromString(
                                                            TEST_TAG_2_URN)))))),
                        new EditableSchemaFieldInfo()
                            .setFieldPath("field1")
                            .setGlobalTags(
                                new GlobalTags()
                                    .setTags(
                                        new TagAssociationArray(
                                            ImmutableList.of(
                                                new TagAssociation()
                                                    .setTag(
                                                        TagUrn.createFromString(TEST_TAG_1_URN)),
                                                new TagAssociation()
                                                    .setTag(
                                                        TagUrn.createFromString(
                                                            TEST_TAG_2_URN)))))))));

    final GlobalTags oldTags =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_1_URN)),
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_2_URN)))));

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(SCHEMA_METADATA_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldSchema);
    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(EDITABLE_SCHEMA_METADATA_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldEditableSchema);
    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(TEST_SCHEMA_FIELD_URN),
                Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldTags);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TAG_1_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(TEST_SCHEMA_FIELD_URN), eq(true))).thenReturn(true);

    RemoveTagResolver resolver = new RemoveTagResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    TagAssociationInput input =
        new TagAssociationInput(
            TEST_TAG_1_URN, TEST_ENTITY_URN, SubResourceType.DATASET_FIELD, TEST_FIELD_PATH);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    final EditableSchemaMetadata newEditableSchema =
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(
                new EditableSchemaFieldInfoArray(
                    ImmutableList.of(
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH)
                            .setGlobalTags(
                                new GlobalTags()
                                    .setTags(
                                        new TagAssociationArray(
                                            ImmutableList.of(
                                                new TagAssociation()
                                                    .setTag(
                                                        TagUrn.createFromString(
                                                            TEST_TAG_2_URN)))))),
                        new EditableSchemaFieldInfo()
                            .setFieldPath("field1")
                            .setGlobalTags(
                                new GlobalTags()
                                    .setTags(
                                        new TagAssociationArray(
                                            ImmutableList.of(
                                                new TagAssociation()
                                                    .setTag(
                                                        TagUrn.createFromString(TEST_TAG_1_URN)),
                                                new TagAssociation()
                                                    .setTag(
                                                        TagUrn.createFromString(
                                                            TEST_TAG_2_URN)))))))));

    final GlobalTags updatedTags =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_2_URN)))));

    final MetadataChangeProposal globalTagsProposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            TEST_SCHEMA_FIELD_URN, GLOBAL_TAGS_ASPECT_NAME, updatedTags);

    final MetadataChangeProposal editableSchemaMetadataProposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN),
            EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
            newEditableSchema);

    verifyIngestProposal(
        mockService, 1, List.of(globalTagsProposal, editableSchemaMetadataProposal));
  }

  @Test
  public void testGetSuccessSchemaFieldUrn() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    final SchemaMetadata oldSchema =
        new SchemaMetadata()
            .setFields(
                new SchemaFieldArray(
                    ImmutableList.of(
                        new SchemaField().setFieldPath(TEST_FIELD_PATH),
                        new SchemaField().setFieldPath("field1"))));

    final EditableSchemaMetadata oldEditableSchema =
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(
                new EditableSchemaFieldInfoArray(
                    ImmutableList.of(
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH)
                            .setGlobalTags(
                                new GlobalTags()
                                    .setTags(
                                        new TagAssociationArray(
                                            ImmutableList.of(
                                                new TagAssociation()
                                                    .setTag(
                                                        TagUrn.createFromString(TEST_TAG_1_URN)),
                                                new TagAssociation()
                                                    .setTag(
                                                        TagUrn.createFromString(
                                                            TEST_TAG_2_URN)))))),
                        new EditableSchemaFieldInfo()
                            .setFieldPath("field1")
                            .setGlobalTags(
                                new GlobalTags()
                                    .setTags(
                                        new TagAssociationArray(
                                            ImmutableList.of(
                                                new TagAssociation()
                                                    .setTag(
                                                        TagUrn.createFromString(TEST_TAG_1_URN)),
                                                new TagAssociation()
                                                    .setTag(
                                                        TagUrn.createFromString(
                                                            TEST_TAG_2_URN)))))))));

    final GlobalTags oldTags =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_1_URN)),
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_2_URN)))));

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(SCHEMA_METADATA_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldSchema);
    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(EDITABLE_SCHEMA_METADATA_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldEditableSchema);
    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(TEST_SCHEMA_FIELD_URN),
                Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldTags);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TAG_1_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(TEST_SCHEMA_FIELD_URN), eq(true))).thenReturn(true);

    RemoveTagResolver resolver = new RemoveTagResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    TagAssociationInput input =
        new TagAssociationInput(TEST_TAG_1_URN, TEST_SCHEMA_FIELD_URN.toString(), null, null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    final EditableSchemaMetadata newEditableSchema =
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(
                new EditableSchemaFieldInfoArray(
                    ImmutableList.of(
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH)
                            .setGlobalTags(
                                new GlobalTags()
                                    .setTags(
                                        new TagAssociationArray(
                                            ImmutableList.of(
                                                new TagAssociation()
                                                    .setTag(
                                                        TagUrn.createFromString(
                                                            TEST_TAG_2_URN)))))),
                        new EditableSchemaFieldInfo()
                            .setFieldPath("field1")
                            .setGlobalTags(
                                new GlobalTags()
                                    .setTags(
                                        new TagAssociationArray(
                                            ImmutableList.of(
                                                new TagAssociation()
                                                    .setTag(
                                                        TagUrn.createFromString(TEST_TAG_1_URN)),
                                                new TagAssociation()
                                                    .setTag(
                                                        TagUrn.createFromString(
                                                            TEST_TAG_2_URN)))))))));

    final GlobalTags updatedTags =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_2_URN)))));

    final MetadataChangeProposal globalTagsProposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            TEST_SCHEMA_FIELD_URN, GLOBAL_TAGS_ASPECT_NAME, updatedTags);

    final MetadataChangeProposal editableSchemaMetadataProposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN),
            EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
            newEditableSchema);

    verifyIngestProposal(
        mockService, 1, List.of(globalTagsProposal, editableSchemaMetadataProposal));
  }

  @Test
  public void testGetFailureResourceDoesNotExist() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(false);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TAG_1_URN)), eq(true)))
        .thenReturn(true);

    RemoveTagResolver resolver = new RemoveTagResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    TagAssociationInput input =
        new TagAssociationInput(TEST_TAG_1_URN, TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0))
        .ingestProposal(any(), Mockito.any(AspectsBatchImpl.class), Mockito.anyBoolean());
  }

  @Test
  public void testGetFailureTagDoesNotExist() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TAG_1_URN)), eq(true)))
        .thenReturn(false);

    RemoveTagResolver resolver = new RemoveTagResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    TagAssociationInput input =
        new TagAssociationInput(TEST_TAG_1_URN, TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    RemoveTagResolver resolver = new RemoveTagResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    TagAssociationInput input =
        new TagAssociationInput(TEST_TAG_1_URN, TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(AuthorizationException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0))
        .ingestProposal(any(), Mockito.any(AspectsBatchImpl.class), Mockito.anyBoolean());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    // We need to mock exists calls first so validation doesn't fail first
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TAG_1_URN)), eq(true)))
        .thenReturn(true);

    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .ingestProposal(any(), Mockito.any(AspectsBatchImpl.class), Mockito.anyBoolean());

    RemoveTagResolver resolver = new RemoveTagResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    TagAssociationInput input =
        new TagAssociationInput(TEST_TAG_1_URN, TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
