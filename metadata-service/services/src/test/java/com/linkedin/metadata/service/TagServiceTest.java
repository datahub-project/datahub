package com.linkedin.metadata.service;

import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.resource.SubResourceType;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TagServiceTest {

  private static final Urn TEST_ENTITY_URN_1 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test,PROD)");
  private static final Urn TEST_ENTITY_URN_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test1,PROD)");

  private static final String TEST_FIELD_PATH_1 = "myfield1";
  private static final String TEST_FIELD_PATH_2 = "myfield2";
  private static final Urn TEST_SCHEMA_FIELD_URN_1 =
      SchemaFieldUtils.generateSchemaFieldUrn(TEST_ENTITY_URN_1, TEST_FIELD_PATH_1);
  private static final Urn TEST_SCHEMA_FIELD_URN_2 =
      SchemaFieldUtils.generateSchemaFieldUrn(TEST_ENTITY_URN_2, TEST_FIELD_PATH_2);

  private static final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();

  @Test
  public void testAddGlobalTagsToEntity() throws Exception {
    final TagService service = createMockTagService();

    Urn newTagUrn = UrnUtils.getUrn("urn:li:tag:newTag");
    List<MetadataChangeProposal> events =
        service.batchAddTags(
            opContext,
            ImmutableList.of(newTagUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            null);

    String expectedEntityTagPatch =
        String.format(
            "[{\"op\":\"add\",\"path\":\"/tags/%s\",\"value\":{\"tag\":\"%s\"}}]",
            newTagUrn, newTagUrn);

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getEntityUrn(), TEST_ENTITY_URN_1);
    Assert.assertEquals(event1.getAspectName(), Constants.GLOBAL_TAGS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);

    Assert.assertEquals(
        event1.getAspect().getValue().asString(StandardCharsets.UTF_8), expectedEntityTagPatch);

    MetadataChangeProposal event2 = events.get(1);
    Assert.assertEquals(event2.getEntityUrn(), TEST_ENTITY_URN_2);
    Assert.assertEquals(event2.getAspectName(), Constants.GLOBAL_TAGS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);

    Assert.assertEquals(
        event2.getAspect().getValue().asString(StandardCharsets.UTF_8), expectedEntityTagPatch);
  }

  @Test
  public void testAddGlobalTagsToSchemaField() throws Exception {
    final TagService service = createMockTagService();

    Urn newTagUrn = UrnUtils.getUrn("urn:li:tag:newTag");
    List<MetadataChangeProposal> events =
        service.batchAddTags(
            opContext,
            ImmutableList.of(newTagUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "myfield1"),
                new ResourceReference(
                    TEST_ENTITY_URN_2, SubResourceType.DATASET_FIELD, "myfield2")),
            null);

    String expectedSchemaFieldTagPatch1 =
        String.format(
            "[{\"op\":\"add\",\"path\":\"/editableSchemaFieldInfo/myfield1/globalTags/tags/%s\",\"value\":{\"tag\":\"%s\"}}]",
            newTagUrn, newTagUrn);

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getEntityUrn(), TEST_ENTITY_URN_1);
    Assert.assertEquals(event1.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);

    Assert.assertEquals(
        event1.getAspect().getValue().asString(StandardCharsets.UTF_8),
        expectedSchemaFieldTagPatch1);

    MetadataChangeProposal event2 = events.get(1);
    Assert.assertEquals(event2.getEntityUrn(), TEST_ENTITY_URN_2);
    Assert.assertEquals(event2.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);

    String expectedSchemaFieldTagPatch2 =
        String.format(
            "[{\"op\":\"add\",\"path\":\"/editableSchemaFieldInfo/myfield2/globalTags/tags/%s\",\"value\":{\"tag\":\"%s\"}}]",
            newTagUrn, newTagUrn);

    Assert.assertEquals(
        event2.getAspect().getValue().asString(StandardCharsets.UTF_8),
        expectedSchemaFieldTagPatch2);
  }

  @Test
  public void testRemoveGlobalTagsToEntity() throws Exception {
    final TagService service = createMockTagService();

    Urn newTagUrn = UrnUtils.getUrn("urn:li:tag:newTag");
    List<MetadataChangeProposal> events =
        service.batchRemoveTags(
            opContext,
            ImmutableList.of(newTagUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            null);

    String expectedEntityTagPatch =
        String.format("[{\"op\":\"remove\",\"path\":\"/tags/%s\",\"value\":null}]", newTagUrn);

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getEntityUrn(), TEST_ENTITY_URN_1);
    Assert.assertEquals(event1.getAspectName(), Constants.GLOBAL_TAGS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);

    Assert.assertEquals(
        event1.getAspect().getValue().asString(StandardCharsets.UTF_8), expectedEntityTagPatch);

    MetadataChangeProposal event2 = events.get(1);
    Assert.assertEquals(event2.getEntityUrn(), TEST_ENTITY_URN_2);
    Assert.assertEquals(event2.getAspectName(), Constants.GLOBAL_TAGS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);

    Assert.assertEquals(
        event2.getAspect().getValue().asString(StandardCharsets.UTF_8), expectedEntityTagPatch);
  }

  @Test
  public void testRemoveGlobalTagsToSchemaField() throws Exception {
    final TagService service = createMockTagService();

    Urn newTagUrn = UrnUtils.getUrn("urn:li:tag:newTag");
    List<MetadataChangeProposal> events =
        service.batchRemoveTags(
            opContext,
            ImmutableList.of(newTagUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "myfield1"),
                new ResourceReference(
                    TEST_ENTITY_URN_2, SubResourceType.DATASET_FIELD, "myfield2")),
            null);

    Assert.assertEquals(events.size(), 4);

    String expectedEntityTagPatch =
        String.format("[{\"op\":\"remove\",\"path\":\"/tags/%s\",\"value\":null}]", newTagUrn);

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getEntityUrn(), TEST_SCHEMA_FIELD_URN_1);
    Assert.assertEquals(event1.getAspectName(), Constants.GLOBAL_TAGS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.SCHEMA_FIELD_ENTITY_NAME);

    Assert.assertEquals(
        event1.getAspect().getValue().asString(StandardCharsets.UTF_8), expectedEntityTagPatch);

    MetadataChangeProposal event2 = events.get(1);
    Assert.assertEquals(event2.getEntityUrn(), TEST_SCHEMA_FIELD_URN_2);
    Assert.assertEquals(event2.getAspectName(), Constants.GLOBAL_TAGS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.SCHEMA_FIELD_ENTITY_NAME);

    Assert.assertEquals(
        event2.getAspect().getValue().asString(StandardCharsets.UTF_8), expectedEntityTagPatch);

    MetadataChangeProposal event3 = events.get(2);
    Assert.assertEquals(event3.getEntityUrn(), TEST_ENTITY_URN_1);
    Assert.assertEquals(event3.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event3.getEntityType(), Constants.DATASET_ENTITY_NAME);

    String expectedSchemaFieldTagPatch1 =
        String.format(
            "[{\"op\":\"remove\",\"path\":\"/editableSchemaFieldInfo/myfield1/globalTags/tags/%s\",\"value\":null}]",
            newTagUrn);

    Assert.assertEquals(
        event3.getAspect().getValue().asString(StandardCharsets.UTF_8),
        expectedSchemaFieldTagPatch1);

    MetadataChangeProposal event4 = events.get(3);
    Assert.assertEquals(event4.getEntityUrn(), TEST_ENTITY_URN_2);
    Assert.assertEquals(event4.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event4.getEntityType(), Constants.DATASET_ENTITY_NAME);

    String expectedSchemaFieldTagPatch2 =
        String.format(
            "[{\"op\":\"remove\",\"path\":\"/editableSchemaFieldInfo/myfield2/globalTags/tags/%s\",\"value\":null}]",
            newTagUrn);

    Assert.assertEquals(
        event4.getAspect().getValue().asString(StandardCharsets.UTF_8),
        expectedSchemaFieldTagPatch2);
  }

  @Test
  public void testRemoveGlobalTagsSchemaFieldUrn() throws Exception {
    final TagService service = createMockTagService();

    Urn newTagUrn = UrnUtils.getUrn("urn:li:tag:newTag");
    List<MetadataChangeProposal> events =
        service.batchRemoveTags(
            opContext,
            ImmutableList.of(newTagUrn),
            ImmutableList.of(
                new ResourceReference(TEST_SCHEMA_FIELD_URN_1, null, null),
                new ResourceReference(TEST_SCHEMA_FIELD_URN_2, null, null)),
            null);

    Assert.assertEquals(events.size(), 4);

    String expectedEntityTagPatch =
        String.format("[{\"op\":\"remove\",\"path\":\"/tags/%s\",\"value\":null}]", newTagUrn);

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getEntityUrn(), TEST_SCHEMA_FIELD_URN_1);
    Assert.assertEquals(event1.getAspectName(), Constants.GLOBAL_TAGS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.SCHEMA_FIELD_ENTITY_NAME);

    Assert.assertEquals(
        event1.getAspect().getValue().asString(StandardCharsets.UTF_8), expectedEntityTagPatch);

    MetadataChangeProposal event2 = events.get(1);
    Assert.assertEquals(event2.getEntityUrn(), TEST_SCHEMA_FIELD_URN_2);
    Assert.assertEquals(event2.getAspectName(), Constants.GLOBAL_TAGS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.SCHEMA_FIELD_ENTITY_NAME);

    Assert.assertEquals(
        event2.getAspect().getValue().asString(StandardCharsets.UTF_8), expectedEntityTagPatch);

    MetadataChangeProposal event3 = events.get(2);
    Assert.assertEquals(event3.getEntityUrn(), TEST_ENTITY_URN_1);
    Assert.assertEquals(event3.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event3.getEntityType(), Constants.DATASET_ENTITY_NAME);

    String expectedSchemaFieldTagPatch1 =
        String.format(
            "[{\"op\":\"remove\",\"path\":\"/editableSchemaFieldInfo/myfield1/globalTags/tags/%s\",\"value\":null}]",
            newTagUrn);

    Assert.assertEquals(
        event3.getAspect().getValue().asString(StandardCharsets.UTF_8),
        expectedSchemaFieldTagPatch1);

    MetadataChangeProposal event4 = events.get(3);
    Assert.assertEquals(event4.getEntityUrn(), TEST_ENTITY_URN_2);
    Assert.assertEquals(event4.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event4.getEntityType(), Constants.DATASET_ENTITY_NAME);

    String expectedSchemaFieldTagPatch2 =
        String.format(
            "[{\"op\":\"remove\",\"path\":\"/editableSchemaFieldInfo/myfield2/globalTags/tags/%s\",\"value\":null}]",
            newTagUrn);

    Assert.assertEquals(
        event4.getAspect().getValue().asString(StandardCharsets.UTF_8),
        expectedSchemaFieldTagPatch2);
  }

  @Test
  public void testGetEntityTags() throws Exception {
    final TagService service = createMockTagService();
    final Urn tagUrn1 = UrnUtils.getUrn("urn:li:tag:existingTag1");
    final Urn tagUrn2 = UrnUtils.getUrn("urn:li:tag:existingTag2");

    final GlobalTags existingTags = new GlobalTags();
    existingTags.setTags(
        new TagAssociationArray(
            ImmutableList.of(
                new TagAssociation().setTag(TagUrn.createFromUrn(tagUrn1)),
                new TagAssociation().setTag(TagUrn.createFromUrn(tagUrn2)))));

    Mockito.when(
            service.entityClient.getV2(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_ENTITY_URN_1.getEntityType()),
                Mockito.eq(TEST_ENTITY_URN_1),
                Mockito.eq(ImmutableSet.of(Constants.GLOBAL_TAGS_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.GLOBAL_TAGS_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(existingTags.data()))))));

    final List<TagAssociation> tags = service.getEntityTags(opContext, TEST_ENTITY_URN_1);

    Assert.assertEquals(tags.size(), 2);
    Assert.assertEquals(tags.get(0).getTag().toString(), tagUrn1.toString());
    Assert.assertEquals(tags.get(1).getTag().toString(), tagUrn2.toString());
  }

  @Test
  public void testGetEntityTagsNone() throws Exception {
    final TagService service = createMockTagService();

    Mockito.when(
            service.entityClient.getV2(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_ENTITY_URN_1.getEntityType()),
                Mockito.eq(TEST_ENTITY_URN_1),
                Mockito.eq(ImmutableSet.of(Constants.GLOBAL_TAGS_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse().setAspects(new EnvelopedAspectMap(Collections.emptyMap())));

    final List<TagAssociation> tags = service.getEntityTags(opContext, TEST_ENTITY_URN_1);

    Assert.assertEquals(tags.size(), 0);
  }

  @Test
  public void testGetSchemaFieldTagsOnlyEditableSchemaMetadata() throws Exception {
    final TagService service = createMockTagService();

    final String testFieldPath = "testField";
    final Urn tagUrn1 = UrnUtils.getUrn("urn:li:tag:existingTag1");
    final Urn tagUrn2 = UrnUtils.getUrn("urn:li:tag:existingTag2");

    final EditableSchemaMetadata existingEditableSchemaMetadata = new EditableSchemaMetadata();
    existingEditableSchemaMetadata.setEditableSchemaFieldInfo(
        new EditableSchemaFieldInfoArray(
            ImmutableList.of(
                new EditableSchemaFieldInfo()
                    .setFieldPath(testFieldPath)
                    .setGlobalTags(
                        new GlobalTags()
                            .setTags(
                                new TagAssociationArray(
                                    ImmutableList.of(
                                        new TagAssociation().setTag(TagUrn.createFromUrn(tagUrn1)),
                                        new TagAssociation()
                                            .setTag(TagUrn.createFromUrn(tagUrn2)))))))));

    Mockito.when(
            service.entityClient.getV2(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_ENTITY_URN_1.getEntityType()),
                Mockito.eq(TEST_ENTITY_URN_1),
                Mockito.eq(ImmutableSet.of(Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(existingEditableSchemaMetadata.data()))))));

    Mockito.when(
            service.entityClient.getV2(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_ENTITY_URN_1.getEntityType()),
                Mockito.eq(TEST_ENTITY_URN_1),
                Mockito.eq(ImmutableSet.of(Constants.SCHEMA_METADATA_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse().setAspects(new EnvelopedAspectMap(Collections.emptyMap())));

    final List<TagAssociation> tags =
        service.getSchemaFieldTags(opContext, TEST_ENTITY_URN_1, testFieldPath);

    Assert.assertEquals(tags.size(), 2);
    Assert.assertEquals(tags.get(0).getTag().toString(), tagUrn1.toString());
    Assert.assertEquals(tags.get(1).getTag().toString(), tagUrn2.toString());
  }

  @Test
  public void testGetSchemaFieldTagsOnlySchemaMetadata() throws Exception {
    final TagService service = createMockTagService();

    final String testFieldPath = "testField";
    final Urn tagUrn1 = UrnUtils.getUrn("urn:li:tag:existingTag1");
    final Urn tagUrn2 = UrnUtils.getUrn("urn:li:tag:existingTag2");

    final SchemaMetadata existingSchemaMetadata = new SchemaMetadata();
    existingSchemaMetadata.setFields(
        new SchemaFieldArray(
            ImmutableList.of(
                new SchemaField()
                    .setFieldPath(testFieldPath)
                    .setGlobalTags(
                        new GlobalTags()
                            .setTags(
                                new TagAssociationArray(
                                    ImmutableList.of(
                                        new TagAssociation().setTag(TagUrn.createFromUrn(tagUrn1)),
                                        new TagAssociation()
                                            .setTag(TagUrn.createFromUrn(tagUrn2)))))))));

    Mockito.when(
            service.entityClient.getV2(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_ENTITY_URN_1.getEntityType()),
                Mockito.eq(TEST_ENTITY_URN_1),
                Mockito.eq(ImmutableSet.of(Constants.SCHEMA_METADATA_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.SCHEMA_METADATA_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(existingSchemaMetadata.data()))))));

    Mockito.when(
            service.entityClient.getV2(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_ENTITY_URN_1.getEntityType()),
                Mockito.eq(TEST_ENTITY_URN_1),
                Mockito.eq(ImmutableSet.of(Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse().setAspects(new EnvelopedAspectMap(Collections.emptyMap())));

    final List<TagAssociation> tags =
        service.getSchemaFieldTags(opContext, TEST_ENTITY_URN_1, testFieldPath);

    Assert.assertEquals(tags.size(), 2);
    Assert.assertEquals(tags.get(0).getTag().toString(), tagUrn1.toString());
    Assert.assertEquals(tags.get(1).getTag().toString(), tagUrn2.toString());
  }

  @Test
  public void testGetSchemaFieldTagsOnlyGlobalTags() throws Exception {
    final TagService service = createMockTagService();

    final Urn tagUrn1 = UrnUtils.getUrn("urn:li:tag:existingTag1");
    final Urn tagUrn2 = UrnUtils.getUrn("urn:li:tag:existingTag2");

    final GlobalTags existingTags =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(
                        new TagAssociation().setTag(TagUrn.createFromUrn(tagUrn1)),
                        new TagAssociation().setTag(TagUrn.createFromUrn(tagUrn2)))));

    Mockito.when(
            service.entityClient.getV2(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_SCHEMA_FIELD_URN_1.getEntityType()),
                Mockito.eq(TEST_SCHEMA_FIELD_URN_1),
                Mockito.eq(ImmutableSet.of(Constants.GLOBAL_TAGS_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.GLOBAL_TAGS_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(existingTags.data()))))));

    final List<TagAssociation> tags =
        service.getSchemaFieldTags(opContext, TEST_ENTITY_URN_1, TEST_FIELD_PATH_1);

    Assert.assertEquals(tags.size(), 2);
    Assert.assertEquals(tags.get(0).getTag().toString(), tagUrn1.toString());
    Assert.assertEquals(tags.get(1).getTag().toString(), tagUrn2.toString());
  }

  @Test
  public void testGetSchemaFieldTagsAll() throws Exception {
    final TagService service = createMockTagService();

    final Urn tagUrn1 = UrnUtils.getUrn("urn:li:tag:existingTag1");
    final Urn tagUrn2 = UrnUtils.getUrn("urn:li:tag:existingTag2");
    final Urn tagUrn3 = UrnUtils.getUrn("urn:li:tag:existingTag3");

    final EditableSchemaMetadata existingEditableSchemaMetadata = new EditableSchemaMetadata();
    existingEditableSchemaMetadata.setEditableSchemaFieldInfo(
        new EditableSchemaFieldInfoArray(
            ImmutableList.of(
                new EditableSchemaFieldInfo()
                    .setFieldPath(TEST_FIELD_PATH_1)
                    .setGlobalTags(
                        new GlobalTags()
                            .setTags(
                                new TagAssociationArray(
                                    ImmutableList.of(
                                        new TagAssociation()
                                            .setTag(TagUrn.createFromUrn(tagUrn1)))))))));

    final SchemaMetadata existingSchemaMetadata = new SchemaMetadata();
    existingSchemaMetadata.setFields(
        new SchemaFieldArray(
            ImmutableList.of(
                new SchemaField()
                    .setFieldPath(TEST_FIELD_PATH_1)
                    .setGlobalTags(
                        new GlobalTags()
                            .setTags(
                                new TagAssociationArray(
                                    ImmutableList.of(
                                        new TagAssociation()
                                            .setTag(TagUrn.createFromUrn(tagUrn2)))))))));

    final GlobalTags existingTags =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(new TagAssociation().setTag(TagUrn.createFromUrn(tagUrn3)))));

    Mockito.when(
            service.entityClient.getV2(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_ENTITY_URN_1.getEntityType()),
                Mockito.eq(TEST_ENTITY_URN_1),
                Mockito.eq(ImmutableSet.of(Constants.SCHEMA_METADATA_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.SCHEMA_METADATA_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(existingSchemaMetadata.data()))))));

    Mockito.when(
            service.entityClient.getV2(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_ENTITY_URN_1.getEntityType()),
                Mockito.eq(TEST_ENTITY_URN_1),
                Mockito.eq(ImmutableSet.of(Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(existingEditableSchemaMetadata.data()))))));

    Mockito.when(
            service.entityClient.getV2(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_SCHEMA_FIELD_URN_1.getEntityType()),
                Mockito.eq(TEST_SCHEMA_FIELD_URN_1),
                Mockito.eq(ImmutableSet.of(Constants.GLOBAL_TAGS_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.GLOBAL_TAGS_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(existingTags.data()))))));

    final List<TagAssociation> tags =
        service.getSchemaFieldTags(opContext, TEST_ENTITY_URN_1, TEST_FIELD_PATH_1);

    Assert.assertEquals(tags.size(), 3);
    Assert.assertEquals(tags.get(0).getTag().toString(), tagUrn3.toString());
    Assert.assertEquals(tags.get(1).getTag().toString(), tagUrn1.toString());
    Assert.assertEquals(tags.get(2).getTag().toString(), tagUrn2.toString());
  }

  @Test
  public void testGetSchemaFieldTagsNoTags() throws Exception {
    final TagService service = createMockTagService();

    Mockito.when(
            service.entityClient.getV2(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_ENTITY_URN_1.getEntityType()),
                Mockito.eq(TEST_ENTITY_URN_1),
                Mockito.eq(ImmutableSet.of(Constants.SCHEMA_METADATA_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse().setAspects(new EnvelopedAspectMap(Collections.emptyMap())));

    Mockito.when(
            service.entityClient.getV2(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_ENTITY_URN_1.getEntityType()),
                Mockito.eq(TEST_ENTITY_URN_1),
                Mockito.eq(ImmutableSet.of(Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse().setAspects(new EnvelopedAspectMap(Collections.emptyMap())));

    Mockito.when(
            service.entityClient.getV2(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_SCHEMA_FIELD_URN_1.getEntityType()),
                Mockito.eq(TEST_SCHEMA_FIELD_URN_1),
                Mockito.eq(ImmutableSet.of(Constants.GLOBAL_TAGS_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse().setAspects(new EnvelopedAspectMap(Collections.emptyMap())));

    final List<TagAssociation> tags =
        service.getSchemaFieldTags(opContext, TEST_ENTITY_URN_1, TEST_FIELD_PATH_1);

    Assert.assertEquals(tags.size(), 0);
  }

  private static TagService createMockTagService() {
    return new TagService(
        mock(SystemEntityClient.class), mock(OpenApiClient.class), opContext.getObjectMapper());
  }
}
