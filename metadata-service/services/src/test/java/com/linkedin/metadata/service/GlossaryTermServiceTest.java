package com.linkedin.metadata.service;

import static com.linkedin.metadata.service.util.ServiceTestUtils.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.resource.SubResourceType;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import io.datahubproject.openapi.client.OpenApiClient;
import java.util.Collections;
import java.util.List;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GlossaryTermServiceTest {

  private static final Urn TEST_GLOSSARY_TERM_URN_1 = UrnUtils.getUrn("urn:li:glossaryTerm:test");
  private static final Urn TEST_GLOSSARY_TERM_URN_2 = UrnUtils.getUrn("urn:li:glossaryTerm:test2");

  @Test
  private void testAddGlossaryTermToEntityExistingGlossaryTerm() throws Exception {
    GlossaryTerms existingGlossaryTerms = new GlossaryTerms();
    existingGlossaryTerms.setTerms(
        new GlossaryTermAssociationArray(
            ImmutableList.of(
                new GlossaryTermAssociation()
                    .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_1)))));
    OpenApiClient mockClient = createMockGlossaryClient(existingGlossaryTerms);

    final GlossaryTermService service = createGlossaryService(mockClient);

    Urn newGlossaryTermUrn = UrnUtils.getUrn("urn:li:glossaryTerm:newGlossaryTerm");
    List<MetadataChangeProposal> events =
        service.buildAddGlossaryTermsProposals(
            ImmutableList.of(newGlossaryTermUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            mockAuthentication());

    GlossaryTermAssociationArray expected =
        new GlossaryTermAssociationArray(
            ImmutableList.of(
                new GlossaryTermAssociation()
                    .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_1)),
                new GlossaryTermAssociation()
                    .setUrn(GlossaryTermUrn.createFromUrn(newGlossaryTermUrn))));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.GLOSSARY_TERMS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    GlossaryTerms glossaryTermsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(),
            event1.getAspect().getContentType(),
            GlossaryTerms.class);
    Assert.assertEquals(glossaryTermsAspect1.getTerms(), expected);

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.GLOSSARY_TERMS_ASPECT_NAME);
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
    OpenApiClient mockClient = createMockGlossaryClient(null);

    final GlossaryTermService service = createGlossaryService(mockClient);

    Urn newGlossaryTermUrn = UrnUtils.getUrn("urn:li:glossaryTerm:newGlossaryTerm");
    List<MetadataChangeProposal> events =
        service.buildAddGlossaryTermsProposals(
            ImmutableList.of(newGlossaryTermUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            mockAuthentication());

    GlossaryTermAssociationArray expectedTermsArray =
        new GlossaryTermAssociationArray(
            ImmutableList.of(
                new GlossaryTermAssociation()
                    .setUrn(GlossaryTermUrn.createFromUrn(newGlossaryTermUrn))));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.GLOSSARY_TERMS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    GlossaryTerms glossaryTermsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(),
            event1.getAspect().getContentType(),
            GlossaryTerms.class);
    Assert.assertEquals(glossaryTermsAspect1.getTerms(), expectedTermsArray);

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.GLOSSARY_TERMS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    GlossaryTerms glossaryTermsAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(),
            event2.getAspect().getContentType(),
            GlossaryTerms.class);
    Assert.assertEquals(glossaryTermsAspect2.getTerms(), expectedTermsArray);
  }

  private GlossaryTermService createGlossaryService(OpenApiClient client) {
    return new GlossaryTermService(
        Mockito.mock(EntityClient.class), Mockito.mock(Authentication.class), client);
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
    OpenApiClient mockClient = createMockSchemaMetadataClient(existingMetadata);

    final GlossaryTermService service = createGlossaryService(mockClient);

    Urn newGlossaryTermUrn = UrnUtils.getUrn("urn:li:glossaryTerm:newGlossaryTerm");
    List<MetadataChangeProposal> events =
        service.buildAddGlossaryTermsProposals(
            ImmutableList.of(newGlossaryTermUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "myfield"),
                new ResourceReference(TEST_ENTITY_URN_2, SubResourceType.DATASET_FIELD, "myfield")),
            mockAuthentication());

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

    MetadataChangeProposal event2 = events.get(0);
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

    OpenApiClient mockClient = createMockSchemaMetadataClient(existingMetadata);

    final GlossaryTermService service = createGlossaryService(mockClient);

    Urn newGlossaryTermUrn = UrnUtils.getUrn("urn:li:glossaryTerm:newGlossaryTerm");
    List<MetadataChangeProposal> events =
        service.buildAddGlossaryTermsProposals(
            ImmutableList.of(newGlossaryTermUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "myfield"),
                new ResourceReference(TEST_ENTITY_URN_2, SubResourceType.DATASET_FIELD, "myfield")),
            mockAuthentication());

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

    MetadataChangeProposal event2 = events.get(0);
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
    OpenApiClient mockClient = createMockGlossaryClient(existingGlossaryTerms);

    final GlossaryTermService service = createGlossaryService(mockClient);

    List<MetadataChangeProposal> events =
        service.buildRemoveGlossaryTermsProposals(
            ImmutableList.of(TEST_GLOSSARY_TERM_URN_1),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            mockAuthentication());

    GlossaryTerms expected =
        new GlossaryTerms()
            .setTerms(
                new GlossaryTermAssociationArray(
                    ImmutableList.of(
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_2)))));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.GLOSSARY_TERMS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate glossaryTermsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(),
            event1.getAspect().getContentType(),
            GlossaryTerms.class);
    Assert.assertEquals(glossaryTermsAspect1, expected);

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.GLOSSARY_TERMS_ASPECT_NAME);
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
    OpenApiClient mockClient = createMockGlossaryClient(null);

    final GlossaryTermService service = createGlossaryService(mockClient);

    Urn newGlossaryTermUrn = UrnUtils.getUrn("urn:li:glossaryTerm:newGlossaryTerm");
    List<MetadataChangeProposal> events =
        service.buildRemoveGlossaryTermsProposals(
            ImmutableList.of(newGlossaryTermUrn),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            mockAuthentication());

    GlossaryTermAssociationArray expected = new GlossaryTermAssociationArray(ImmutableList.of());

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.GLOSSARY_TERMS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    GlossaryTerms glossaryTermsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(),
            event1.getAspect().getContentType(),
            GlossaryTerms.class);
    Assert.assertEquals(glossaryTermsAspect1.getTerms(), expected);

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.GLOSSARY_TERMS_ASPECT_NAME);
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
    OpenApiClient mockClient = createMockSchemaMetadataClient(existingMetadata);

    final GlossaryTermService service = createGlossaryService(mockClient);

    List<MetadataChangeProposal> events =
        service.buildRemoveGlossaryTermsProposals(
            ImmutableList.of(TEST_GLOSSARY_TERM_URN_1),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "myfield"),
                new ResourceReference(TEST_ENTITY_URN_2, SubResourceType.DATASET_FIELD, "myfield")),
            mockAuthentication());

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

    MetadataChangeProposal event2 = events.get(0);
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

    OpenApiClient mockClient = createMockSchemaMetadataClient(existingMetadata);

    final GlossaryTermService service = createGlossaryService(mockClient);

    List<MetadataChangeProposal> events =
        service.buildRemoveGlossaryTermsProposals(
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
        editableSchemaMetadataAspect1
            .getEditableSchemaFieldInfo()
            .get(0)
            .getGlossaryTerms()
            .getTerms(),
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
        editableSchemaMetadataAspect2
            .getEditableSchemaFieldInfo()
            .get(0)
            .getGlossaryTerms()
            .getTerms(),
        Collections.emptyList());
  }
}
