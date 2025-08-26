package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERMS_ASPECT_NAME;
import static com.linkedin.metadata.service.util.ServiceTestUtils.*;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.resource.SubResourceType;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class GlossaryTermServiceTest {

  private static final Urn TEST_GLOSSARY_TERM_URN_1 = UrnUtils.getUrn("urn:li:glossaryTerm:test");
  private static final Urn TEST_GLOSSARY_TERM_URN_2 = UrnUtils.getUrn("urn:li:glossaryTerm:test2");

  private static final Urn TEST_ENTITY_URN_1 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test,PROD)");
  private static final Urn TEST_ENTITY_URN_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test1,PROD)");
  private static AspectRetriever mockAspectRetriever;
  private static OperationContext opContext;

  @BeforeClass
  public void init() {
    mockAspectRetriever = mock(AspectRetriever.class);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization(mockAspectRetriever);
  }

  @Test
  private void testAddGlossaryTermToEntityExistingGlossaryTerm() throws Exception {
    GlossaryTerms existingGlossaryTerms = new GlossaryTerms();
    existingGlossaryTerms.setTerms(
        new GlossaryTermAssociationArray(
            ImmutableList.of(
                new GlossaryTermAssociation()
                    .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_1)))));

    final GlossaryTermService service = createGlossaryService(existingGlossaryTerms);

    Urn newGlossaryTermUrn = UrnUtils.getUrn("urn:li:glossaryTerm:newGlossaryTerm");
    List<MetadataChangeProposal> events =
        service.buildAddGlossaryTermsProposals(
            opContext,
            ImmutableList.of(newGlossaryTermUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            null,
            null);

    GlossaryTermAssociationArray expected =
        new GlossaryTermAssociationArray(
            ImmutableList.of(
                new GlossaryTermAssociation()
                    .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_1)),
                new GlossaryTermAssociation()
                    .setUrn(GlossaryTermUrn.createFromUrn(newGlossaryTermUrn))));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), GLOSSARY_TERMS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    GlossaryTerms glossaryTermsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(),
            event1.getAspect().getContentType(),
            GlossaryTerms.class);
    Assert.assertEquals(glossaryTermsAspect1.getTerms(), expected);

    MetadataChangeProposal event2 = events.get(1);
    Assert.assertEquals(event2.getAspectName(), GLOSSARY_TERMS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    GlossaryTerms glossaryTermsAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(),
            event2.getAspect().getContentType(),
            GlossaryTerms.class);
    Assert.assertEquals(glossaryTermsAspect2.getTerms(), expected);
  }

  @Test
  private void testAddGlossaryTermsToEntityNoExistingGlossaryTerm() throws Exception {
    final GlossaryTermService service = createGlossaryService((EditableSchemaMetadata) null);

    Urn newGlossaryTermUrn = UrnUtils.getUrn("urn:li:glossaryTerm:newGlossaryTerm");
    List<MetadataChangeProposal> events =
        service.buildAddGlossaryTermsProposals(
            opContext,
            ImmutableList.of(newGlossaryTermUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            null,
            null);

    GlossaryTermAssociationArray expectedTermsArray =
        new GlossaryTermAssociationArray(
            ImmutableList.of(
                new GlossaryTermAssociation()
                    .setUrn(GlossaryTermUrn.createFromUrn(newGlossaryTermUrn))));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), GLOSSARY_TERMS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    GlossaryTerms glossaryTermsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(),
            event1.getAspect().getContentType(),
            GlossaryTerms.class);
    Assert.assertEquals(glossaryTermsAspect1.getTerms(), expectedTermsArray);

    MetadataChangeProposal event2 = events.get(1);
    Assert.assertEquals(event2.getAspectName(), GLOSSARY_TERMS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    GlossaryTerms glossaryTermsAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(),
            event2.getAspect().getContentType(),
            GlossaryTerms.class);
    Assert.assertEquals(glossaryTermsAspect2.getTerms(), expectedTermsArray);
  }

  @Test
  private void testAddGlossaryTermToSchemaFieldExistingGlossaryTerm() throws Exception {
    EditableSchemaMetadata existingMetadata = new EditableSchemaMetadata();
    existingMetadata.setEditableSchemaFieldInfo(
        new EditableSchemaFieldInfoArray(
            ImmutableList.of(
                new EditableSchemaFieldInfo()
                    .setFieldPath("myfield")
                    .setGlossaryTerms(
                        new GlossaryTerms()
                            .setTerms(
                                new GlossaryTermAssociationArray(
                                    ImmutableList.of(
                                        new GlossaryTermAssociation()
                                            .setUrn(
                                                GlossaryTermUrn.createFromUrn(
                                                    TEST_GLOSSARY_TERM_URN_1)))))))));

    final GlossaryTermService service = createGlossaryService(existingMetadata);

    Urn newGlossaryTermUrn = UrnUtils.getUrn("urn:li:glossaryTerm:newGlossaryTerm");
    List<MetadataChangeProposal> events =
        service.buildAddGlossaryTermsProposals(
            opContext,
            ImmutableList.of(newGlossaryTermUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "myfield"),
                new ResourceReference(TEST_ENTITY_URN_2, SubResourceType.DATASET_FIELD, "myfield")),
            null,
            null);

    GlossaryTermAssociationArray expected =
        new GlossaryTermAssociationArray(
            ImmutableList.of(
                new GlossaryTermAssociation()
                    .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_1)),
                new GlossaryTermAssociation()
                    .setUrn(GlossaryTermUrn.createFromUrn(newGlossaryTermUrn))));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    EditableSchemaMetadata editableSchemaMetadataAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(),
            event1.getAspect().getContentType(),
            EditableSchemaMetadata.class);
    Assert.assertEquals(
        editableSchemaMetadataAspect1
            .getEditableSchemaFieldInfo()
            .get(0)
            .getGlossaryTerms()
            .getTerms(),
        expected);

    MetadataChangeProposal event2 = events.get(1);
    Assert.assertEquals(event2.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    EditableSchemaMetadata editableSchemaMetadataAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(),
            event2.getAspect().getContentType(),
            EditableSchemaMetadata.class);
    Assert.assertEquals(
        editableSchemaMetadataAspect2
            .getEditableSchemaFieldInfo()
            .get(0)
            .getGlossaryTerms()
            .getTerms(),
        expected);
  }

  @Test
  private void testAddGlossaryTermsToSchemaFieldNoExistingGlossaryTerm() throws Exception {

    EditableSchemaMetadata existingMetadata = new EditableSchemaMetadata();
    existingMetadata.setEditableSchemaFieldInfo(
        new EditableSchemaFieldInfoArray(
            ImmutableList.of(
                new EditableSchemaFieldInfo()
                    .setFieldPath("myfield")
                    .setGlossaryTerms(new GlossaryTerms()))));

    final GlossaryTermService service = createGlossaryService(existingMetadata);

    Urn newGlossaryTermUrn = UrnUtils.getUrn("urn:li:glossaryTerm:newGlossaryTerm");
    List<MetadataChangeProposal> events =
        service.buildAddGlossaryTermsProposals(
            opContext,
            ImmutableList.of(newGlossaryTermUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "myfield"),
                new ResourceReference(TEST_ENTITY_URN_2, SubResourceType.DATASET_FIELD, "myfield")),
            null,
            null);

    GlossaryTermAssociationArray expected =
        new GlossaryTermAssociationArray(
            ImmutableList.of(
                new GlossaryTermAssociation()
                    .setUrn(GlossaryTermUrn.createFromUrn(newGlossaryTermUrn))));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    EditableSchemaMetadata editableSchemaMetadataAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(),
            event1.getAspect().getContentType(),
            EditableSchemaMetadata.class);
    Assert.assertEquals(
        editableSchemaMetadataAspect1
            .getEditableSchemaFieldInfo()
            .get(0)
            .getGlossaryTerms()
            .getTerms(),
        expected);

    MetadataChangeProposal event2 = events.get(1);
    Assert.assertEquals(event2.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    EditableSchemaMetadata editableSchemaMetadataAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(),
            event2.getAspect().getContentType(),
            EditableSchemaMetadata.class);
    Assert.assertEquals(
        editableSchemaMetadataAspect2
            .getEditableSchemaFieldInfo()
            .get(0)
            .getGlossaryTerms()
            .getTerms(),
        expected);
  }

  @Test
  private void testRemoveGlossaryTermToEntityExistingGlossaryTerm() throws Exception {
    GlossaryTerms existingGlossaryTerms = new GlossaryTerms();
    existingGlossaryTerms.setTerms(
        new GlossaryTermAssociationArray(
            ImmutableList.of(
                new GlossaryTermAssociation()
                    .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_1)),
                new GlossaryTermAssociation()
                    .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_2)))));

    final GlossaryTermService service = createGlossaryService(existingGlossaryTerms);

    List<MetadataChangeProposal> events =
        service.buildRemoveGlossaryTermsProposals(
            opContext,
            ImmutableList.of(TEST_GLOSSARY_TERM_URN_1),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)));

    GlossaryTerms expected =
        new GlossaryTerms()
            .setTerms(
                new GlossaryTermAssociationArray(
                    ImmutableList.of(
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_2)))));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), GLOSSARY_TERMS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate glossaryTermsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(),
            event1.getAspect().getContentType(),
            GlossaryTerms.class);
    Assert.assertEquals(glossaryTermsAspect1, expected);

    MetadataChangeProposal event2 = events.get(1);
    Assert.assertEquals(event2.getAspectName(), GLOSSARY_TERMS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate glossaryTermsAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(),
            event2.getAspect().getContentType(),
            GlossaryTerms.class);
    Assert.assertEquals(glossaryTermsAspect2, expected);
  }

  @Test
  private void testRemoveGlossaryTermsToEntityNoExistingGlossaryTerm() throws Exception {
    final GlossaryTermService service = createGlossaryService((GlossaryTerms) null);

    Urn newGlossaryTermUrn = UrnUtils.getUrn("urn:li:glossaryTerm:newGlossaryTerm");
    List<MetadataChangeProposal> events =
        service.buildRemoveGlossaryTermsProposals(
            opContext,
            ImmutableList.of(newGlossaryTermUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)));

    GlossaryTermAssociationArray expected = new GlossaryTermAssociationArray(ImmutableList.of());

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), GLOSSARY_TERMS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    GlossaryTerms glossaryTermsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(),
            event1.getAspect().getContentType(),
            GlossaryTerms.class);
    Assert.assertEquals(glossaryTermsAspect1.getTerms(), expected);

    MetadataChangeProposal event2 = events.get(1);
    Assert.assertEquals(event2.getAspectName(), GLOSSARY_TERMS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    GlossaryTerms glossaryTermsAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(),
            event2.getAspect().getContentType(),
            GlossaryTerms.class);
    Assert.assertEquals(glossaryTermsAspect2.getTerms(), expected);
  }

  @Test
  private void testRemoveGlossaryTermToSchemaFieldExistingGlossaryTerm() throws Exception {
    EditableSchemaMetadata existingMetadata = new EditableSchemaMetadata();
    existingMetadata.setEditableSchemaFieldInfo(
        new EditableSchemaFieldInfoArray(
            ImmutableList.of(
                new EditableSchemaFieldInfo()
                    .setFieldPath("myfield")
                    .setGlossaryTerms(
                        new GlossaryTerms()
                            .setTerms(
                                new GlossaryTermAssociationArray(
                                    ImmutableList.of(
                                        new GlossaryTermAssociation()
                                            .setUrn(
                                                GlossaryTermUrn.createFromUrn(
                                                    TEST_GLOSSARY_TERM_URN_1)),
                                        new GlossaryTermAssociation()
                                            .setUrn(
                                                GlossaryTermUrn.createFromUrn(
                                                    TEST_GLOSSARY_TERM_URN_2)))))))));

    final GlossaryTermService service = createGlossaryService(existingMetadata);

    List<MetadataChangeProposal> events =
        service.buildRemoveGlossaryTermsProposals(
            opContext,
            ImmutableList.of(TEST_GLOSSARY_TERM_URN_1),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "myfield"),
                new ResourceReference(
                    TEST_ENTITY_URN_2, SubResourceType.DATASET_FIELD, "myfield")));

    GlossaryTermAssociationArray expected =
        new GlossaryTermAssociationArray(
            ImmutableList.of(
                new GlossaryTermAssociation()
                    .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_2))));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    EditableSchemaMetadata editableSchemaMetadataAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(),
            event1.getAspect().getContentType(),
            EditableSchemaMetadata.class);
    Assert.assertEquals(
        editableSchemaMetadataAspect1
            .getEditableSchemaFieldInfo()
            .get(0)
            .getGlossaryTerms()
            .getTerms(),
        expected);

    MetadataChangeProposal event2 = events.get(1);
    Assert.assertEquals(event2.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    EditableSchemaMetadata editableSchemaMetadataAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(),
            event2.getAspect().getContentType(),
            EditableSchemaMetadata.class);
    Assert.assertEquals(
        editableSchemaMetadataAspect2
            .getEditableSchemaFieldInfo()
            .get(0)
            .getGlossaryTerms()
            .getTerms(),
        expected);
  }

  @Test
  private void testRemoveGlossaryTermsToSchemaFieldNoExistingGlossaryTerm() throws Exception {

    EditableSchemaMetadata existingMetadata = new EditableSchemaMetadata();
    existingMetadata.setEditableSchemaFieldInfo(
        new EditableSchemaFieldInfoArray(
            ImmutableList.of(
                new EditableSchemaFieldInfo()
                    .setFieldPath("myfield")
                    .setGlossaryTerms(new GlossaryTerms()))));

    final GlossaryTermService service = createGlossaryService(existingMetadata);

    List<MetadataChangeProposal> events =
        service.buildRemoveGlossaryTermsProposals(
            opContext,
            ImmutableList.of(TEST_ENTITY_URN_1),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "myfield"),
                new ResourceReference(
                    TEST_ENTITY_URN_2, SubResourceType.DATASET_FIELD, "myfield")));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    EditableSchemaMetadata editableSchemaMetadataAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(),
            event1.getAspect().getContentType(),
            EditableSchemaMetadata.class);
    Assert.assertEquals(
        editableSchemaMetadataAspect1
            .getEditableSchemaFieldInfo()
            .get(0)
            .getGlossaryTerms()
            .getTerms(),
        Collections.emptyList());

    MetadataChangeProposal event2 = events.get(1);
    Assert.assertEquals(event2.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    EditableSchemaMetadata editableSchemaMetadataAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(),
            event2.getAspect().getContentType(),
            EditableSchemaMetadata.class);
    Assert.assertEquals(
        editableSchemaMetadataAspect2
            .getEditableSchemaFieldInfo()
            .get(0)
            .getGlossaryTerms()
            .getTerms(),
        Collections.emptyList());
  }

  private GlossaryTermService createGlossaryService(
      @Nullable EditableSchemaMetadata existingMetadata) throws Exception {
    setMockAspectRetriever(existingMetadata);
    OpenApiClient mockOpenAPIClient = createMockSchemaMetadataClient(existingMetadata);
    return new GlossaryTermService(
        mock(SystemEntityClient.class), mockOpenAPIClient, opContext.getObjectMapper());
  }

  private GlossaryTermService createGlossaryService(@Nullable GlossaryTerms existingMetadata)
      throws Exception {
    setMockAspectRetriever(existingMetadata);
    OpenApiClient mockOpenAPIClient = createMockGlossaryClient(existingMetadata);
    return new GlossaryTermService(
        mock(SystemEntityClient.class), mockOpenAPIClient, opContext.getObjectMapper());
  }

  private static void setMockAspectRetriever(@Nullable EditableSchemaMetadata existingMetadata) {
    reset(mockAspectRetriever);

    if (existingMetadata != null) {
      Mockito.when(
              mockAspectRetriever.getLatestAspectObjects(
                  eq(Set.of(TEST_ENTITY_URN_1, TEST_ENTITY_URN_2)),
                  eq(Set.of(EDITABLE_SCHEMA_METADATA_ASPECT_NAME))))
          .thenReturn(
              ImmutableMap.of(
                  TEST_ENTITY_URN_1,
                  Map.of(EDITABLE_SCHEMA_METADATA_ASPECT_NAME, new Aspect(existingMetadata.data())),
                  TEST_ENTITY_URN_2,
                  Map.of(
                      EDITABLE_SCHEMA_METADATA_ASPECT_NAME, new Aspect(existingMetadata.data()))));
    } else {
      Mockito.when(mockAspectRetriever.getLatestAspectObjects(anySet(), anySet()))
          .thenReturn(Collections.emptyMap());
    }
  }

  private static void setMockAspectRetriever(@Nullable GlossaryTerms existingGlossaryTerms) {
    reset(mockAspectRetriever);

    if (existingGlossaryTerms != null) {
      Mockito.when(
              mockAspectRetriever.getLatestAspectObjects(
                  eq(Set.of(TEST_ENTITY_URN_1, TEST_ENTITY_URN_2)),
                  eq(Set.of(GLOSSARY_TERMS_ASPECT_NAME))))
          .thenReturn(
              ImmutableMap.of(
                  TEST_ENTITY_URN_1,
                  Map.of(GLOSSARY_TERMS_ASPECT_NAME, new Aspect(existingGlossaryTerms.data())),
                  TEST_ENTITY_URN_2,
                  Map.of(GLOSSARY_TERMS_ASPECT_NAME, new Aspect(existingGlossaryTerms.data()))));
    } else {
      Mockito.when(mockAspectRetriever.getLatestAspectObjects(anySet(), anySet()))
          .thenReturn(Collections.emptyMap());
    }
  }
}
