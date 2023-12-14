package com.linkedin.metadata.service;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.resource.SubResourceType;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TagServiceTest {

  private static final Urn TEST_TAG_URN_1 = UrnUtils.getUrn("urn:li:tag:test");
  private static final Urn TEST_TAG_URN_2 = UrnUtils.getUrn("urn:li:tag:test2");

  private static final Urn TEST_ENTITY_URN_1 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test,PROD)");
  private static final Urn TEST_ENTITY_URN_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test1,PROD)");

  @Test
  private void testAddTagToEntityExistingTag() throws Exception {
    GlobalTags existingGlobalTags = new GlobalTags();
    existingGlobalTags.setTags(
        new TagAssociationArray(
            ImmutableList.of(new TagAssociation().setTag(TagUrn.createFromUrn(TEST_TAG_URN_1)))));
    EntityClient mockClient = createMockGlobalTagsClient(existingGlobalTags);

    final TagService service = new TagService(mockClient, Mockito.mock(Authentication.class));

    Urn newTagUrn = UrnUtils.getUrn("urn:li:tag:newTag");
    List<MetadataChangeProposal> events =
        service.buildAddTagsProposals(
            ImmutableList.of(newTagUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            mockAuthentication());

    TagAssociationArray expected =
        new TagAssociationArray(
            ImmutableList.of(
                new TagAssociation().setTag(TagUrn.createFromUrn(TEST_TAG_URN_1)),
                new TagAssociation().setTag(TagUrn.createFromUrn(newTagUrn))));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.GLOBAL_TAGS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    GlobalTags tagsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(), event1.getAspect().getContentType(), GlobalTags.class);
    Assert.assertEquals(tagsAspect1.getTags(), expected);

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.GLOBAL_TAGS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    GlobalTags tagsAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(), event2.getAspect().getContentType(), GlobalTags.class);
    Assert.assertEquals(tagsAspect2.getTags(), expected);
  }

  @Test
  private void testAddGlobalTagsToEntityNoExistingTag() throws Exception {
    EntityClient mockClient = createMockGlobalTagsClient(null);

    final TagService service = new TagService(mockClient, Mockito.mock(Authentication.class));

    Urn newTagUrn = UrnUtils.getUrn("urn:li:tag:newTag");
    List<MetadataChangeProposal> events =
        service.buildAddTagsProposals(
            ImmutableList.of(newTagUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            mockAuthentication());

    TagAssociationArray expectedTermsArray =
        new TagAssociationArray(
            ImmutableList.of(new TagAssociation().setTag(TagUrn.createFromUrn(newTagUrn))));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.GLOBAL_TAGS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    GlobalTags tagsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(), event1.getAspect().getContentType(), GlobalTags.class);
    Assert.assertEquals(tagsAspect1.getTags(), expectedTermsArray);

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.GLOBAL_TAGS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    GlobalTags tagsAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(), event2.getAspect().getContentType(), GlobalTags.class);
    Assert.assertEquals(tagsAspect2.getTags(), expectedTermsArray);
  }

  @Test
  private void testAddTagToSchemaFieldExistingTag() throws Exception {
    EditableSchemaMetadata existingMetadata = new EditableSchemaMetadata();
    existingMetadata.setEditableSchemaFieldInfo(
        new EditableSchemaFieldInfoArray(
            ImmutableList.of(
                new EditableSchemaFieldInfo()
                    .setFieldPath("myfield")
                    .setGlobalTags(
                        new GlobalTags()
                            .setTags(
                                new TagAssociationArray(
                                    ImmutableList.of(
                                        new TagAssociation()
                                            .setTag(TagUrn.createFromUrn(TEST_TAG_URN_1)))))))));
    EntityClient mockClient = createMockSchemaMetadataEntityClient(existingMetadata);

    final TagService service = new TagService(mockClient, Mockito.mock(Authentication.class));

    Urn newTagUrn = UrnUtils.getUrn("urn:li:tag:newTag");
    List<MetadataChangeProposal> events =
        service.buildAddTagsProposals(
            ImmutableList.of(newTagUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "myfield"),
                new ResourceReference(TEST_ENTITY_URN_2, SubResourceType.DATASET_FIELD, "myfield")),
            mockAuthentication());

    TagAssociationArray expected =
        new TagAssociationArray(
            ImmutableList.of(
                new TagAssociation().setTag(TagUrn.createFromUrn(TEST_TAG_URN_1)),
                new TagAssociation().setTag(TagUrn.createFromUrn(newTagUrn))));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    EditableSchemaMetadata editableSchemaMetadataAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(),
            event1.getAspect().getContentType(),
            EditableSchemaMetadata.class);
    Assert.assertEquals(
        editableSchemaMetadataAspect1.getEditableSchemaFieldInfo().get(0).getGlobalTags().getTags(),
        expected);

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    EditableSchemaMetadata editableSchemaMetadataAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(),
            event2.getAspect().getContentType(),
            EditableSchemaMetadata.class);
    Assert.assertEquals(
        editableSchemaMetadataAspect2.getEditableSchemaFieldInfo().get(0).getGlobalTags().getTags(),
        expected);
  }

  @Test
  private void testAddGlobalTagsToSchemaFieldNoExistingTag() throws Exception {

    EditableSchemaMetadata existingMetadata = new EditableSchemaMetadata();
    existingMetadata.setEditableSchemaFieldInfo(
        new EditableSchemaFieldInfoArray(
            ImmutableList.of(
                new EditableSchemaFieldInfo()
                    .setFieldPath("myfield")
                    .setGlobalTags(new GlobalTags()))));

    EntityClient mockClient = createMockSchemaMetadataEntityClient(existingMetadata);

    final TagService service = new TagService(mockClient, Mockito.mock(Authentication.class));

    Urn newTagUrn = UrnUtils.getUrn("urn:li:tag:newTag");
    List<MetadataChangeProposal> events =
        service.buildAddTagsProposals(
            ImmutableList.of(newTagUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "myfield"),
                new ResourceReference(TEST_ENTITY_URN_2, SubResourceType.DATASET_FIELD, "myfield")),
            mockAuthentication());

    TagAssociationArray expected =
        new TagAssociationArray(
            ImmutableList.of(new TagAssociation().setTag(TagUrn.createFromUrn(newTagUrn))));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    EditableSchemaMetadata editableSchemaMetadataAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(),
            event1.getAspect().getContentType(),
            EditableSchemaMetadata.class);
    Assert.assertEquals(
        editableSchemaMetadataAspect1.getEditableSchemaFieldInfo().get(0).getGlobalTags().getTags(),
        expected);

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    EditableSchemaMetadata editableSchemaMetadataAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(),
            event2.getAspect().getContentType(),
            EditableSchemaMetadata.class);
    Assert.assertEquals(
        editableSchemaMetadataAspect2.getEditableSchemaFieldInfo().get(0).getGlobalTags().getTags(),
        expected);
  }

  @Test
  private void testRemoveTagToEntityExistingTag() throws Exception {
    GlobalTags existingGlobalTags = new GlobalTags();
    existingGlobalTags.setTags(
        new TagAssociationArray(
            ImmutableList.of(
                new TagAssociation().setTag(TagUrn.createFromUrn(TEST_TAG_URN_1)),
                new TagAssociation().setTag(TagUrn.createFromUrn(TEST_TAG_URN_2)))));
    EntityClient mockClient = createMockGlobalTagsClient(existingGlobalTags);

    final TagService service = new TagService(mockClient, Mockito.mock(Authentication.class));

    List<MetadataChangeProposal> events =
        service.buildRemoveTagsProposals(
            ImmutableList.of(TEST_TAG_URN_1),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            mockAuthentication());

    GlobalTags expected =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(
                        new TagAssociation().setTag(TagUrn.createFromUrn(TEST_TAG_URN_2)))));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.GLOBAL_TAGS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate tagsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(), event1.getAspect().getContentType(), GlobalTags.class);
    Assert.assertEquals(tagsAspect1, expected);

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.GLOBAL_TAGS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate tagsAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(), event2.getAspect().getContentType(), GlobalTags.class);
    Assert.assertEquals(tagsAspect2, expected);
  }

  @Test
  private void testRemoveGlobalTagsToEntityNoExistingTag() throws Exception {
    EntityClient mockClient = createMockGlobalTagsClient(null);

    final TagService service = new TagService(mockClient, Mockito.mock(Authentication.class));

    Urn newTagUrn = UrnUtils.getUrn("urn:li:tag:newTag");
    List<MetadataChangeProposal> events =
        service.buildRemoveTagsProposals(
            ImmutableList.of(newTagUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            mockAuthentication());

    TagAssociationArray expected = new TagAssociationArray(ImmutableList.of());

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.GLOBAL_TAGS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    GlobalTags tagsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(), event1.getAspect().getContentType(), GlobalTags.class);
    Assert.assertEquals(tagsAspect1.getTags(), expected);

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.GLOBAL_TAGS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    GlobalTags tagsAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(), event2.getAspect().getContentType(), GlobalTags.class);
    Assert.assertEquals(tagsAspect2.getTags(), expected);
  }

  @Test
  private void testRemoveTagToSchemaFieldExistingTag() throws Exception {
    EditableSchemaMetadata existingMetadata = new EditableSchemaMetadata();
    existingMetadata.setEditableSchemaFieldInfo(
        new EditableSchemaFieldInfoArray(
            ImmutableList.of(
                new EditableSchemaFieldInfo()
                    .setFieldPath("myfield")
                    .setGlobalTags(
                        new GlobalTags()
                            .setTags(
                                new TagAssociationArray(
                                    ImmutableList.of(
                                        new TagAssociation()
                                            .setTag(TagUrn.createFromUrn(TEST_TAG_URN_1)),
                                        new TagAssociation()
                                            .setTag(TagUrn.createFromUrn(TEST_TAG_URN_2)))))))));
    EntityClient mockClient = createMockSchemaMetadataEntityClient(existingMetadata);

    final TagService service = new TagService(mockClient, Mockito.mock(Authentication.class));

    List<MetadataChangeProposal> events =
        service.buildRemoveTagsProposals(
            ImmutableList.of(TEST_TAG_URN_1),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "myfield"),
                new ResourceReference(TEST_ENTITY_URN_2, SubResourceType.DATASET_FIELD, "myfield")),
            mockAuthentication());

    TagAssociationArray expected =
        new TagAssociationArray(
            ImmutableList.of(new TagAssociation().setTag(TagUrn.createFromUrn(TEST_TAG_URN_2))));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    EditableSchemaMetadata editableSchemaMetadataAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(),
            event1.getAspect().getContentType(),
            EditableSchemaMetadata.class);
    Assert.assertEquals(
        editableSchemaMetadataAspect1.getEditableSchemaFieldInfo().get(0).getGlobalTags().getTags(),
        expected);

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    EditableSchemaMetadata editableSchemaMetadataAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(),
            event2.getAspect().getContentType(),
            EditableSchemaMetadata.class);
    Assert.assertEquals(
        editableSchemaMetadataAspect2.getEditableSchemaFieldInfo().get(0).getGlobalTags().getTags(),
        expected);
  }

  @Test
  private void testRemoveGlobalTagsToSchemaFieldNoExistingTag() throws Exception {

    EditableSchemaMetadata existingMetadata = new EditableSchemaMetadata();
    existingMetadata.setEditableSchemaFieldInfo(
        new EditableSchemaFieldInfoArray(
            ImmutableList.of(
                new EditableSchemaFieldInfo()
                    .setFieldPath("myfield")
                    .setGlobalTags(new GlobalTags()))));

    EntityClient mockClient = createMockSchemaMetadataEntityClient(existingMetadata);

    final TagService service = new TagService(mockClient, Mockito.mock(Authentication.class));

    List<MetadataChangeProposal> events =
        service.buildRemoveTagsProposals(
            ImmutableList.of(TEST_ENTITY_URN_1),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "myfield"),
                new ResourceReference(TEST_ENTITY_URN_2, SubResourceType.DATASET_FIELD, "myfield")),
            mockAuthentication());

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    EditableSchemaMetadata editableSchemaMetadataAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(),
            event1.getAspect().getContentType(),
            EditableSchemaMetadata.class);
    Assert.assertEquals(
        editableSchemaMetadataAspect1.getEditableSchemaFieldInfo().get(0).getGlobalTags().getTags(),
        Collections.emptyList());

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    EditableSchemaMetadata editableSchemaMetadataAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(),
            event2.getAspect().getContentType(),
            EditableSchemaMetadata.class);
    Assert.assertEquals(
        editableSchemaMetadataAspect2.getEditableSchemaFieldInfo().get(0).getGlobalTags().getTags(),
        Collections.emptyList());
  }

  private static EntityClient createMockGlobalTagsClient(@Nullable GlobalTags existingGlobalTags)
      throws Exception {
    return createMockEntityClient(existingGlobalTags, Constants.GLOBAL_TAGS_ASPECT_NAME);
  }

  private static EntityClient createMockSchemaMetadataEntityClient(
      @Nullable EditableSchemaMetadata existingMetadata) throws Exception {
    return createMockEntityClient(existingMetadata, Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
  }

  private static EntityClient createMockEntityClient(
      @Nullable RecordTemplate aspect, String aspectName) throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.batchGetV2(
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(ImmutableSet.of(TEST_ENTITY_URN_1, TEST_ENTITY_URN_2)),
                Mockito.eq(ImmutableSet.of(aspectName)),
                Mockito.any(Authentication.class)))
        .thenReturn(
            aspect != null
                ? ImmutableMap.of(
                    TEST_ENTITY_URN_1,
                    new EntityResponse()
                        .setUrn(TEST_ENTITY_URN_1)
                        .setEntityName(Constants.DATASET_ENTITY_NAME)
                        .setAspects(
                            new EnvelopedAspectMap(
                                ImmutableMap.of(
                                    aspectName,
                                    new EnvelopedAspect().setValue(new Aspect(aspect.data()))))),
                    TEST_ENTITY_URN_2,
                    new EntityResponse()
                        .setUrn(TEST_ENTITY_URN_2)
                        .setEntityName(Constants.DATASET_ENTITY_NAME)
                        .setAspects(
                            new EnvelopedAspectMap(
                                ImmutableMap.of(
                                    aspectName,
                                    new EnvelopedAspect().setValue(new Aspect(aspect.data()))))))
                : Collections.emptyMap());
    return mockClient;
  }

  private static Authentication mockAuthentication() {
    Authentication mockAuth = Mockito.mock(Authentication.class);
    Mockito.when(mockAuth.getActor()).thenReturn(new Actor(ActorType.USER, Constants.SYSTEM_ACTOR));
    return mockAuth;
  }
}
