package com.linkedin.metadata.service;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.urn.GlossaryTermUrn;
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
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;


public class GlossaryTermServiceTest {

  private static final Urn TEST_GLOSSARY_TERM_URN_1 = UrnUtils.getUrn("urn:li:glossaryTerm:test");
  private static final Urn TEST_GLOSSARY_TERM_URN_2 = UrnUtils.getUrn("urn:li:glossaryTerm:test2");

  private static final Urn TEST_ENTITY_URN_1 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test,PROD)");
  private static final Urn TEST_ENTITY_URN_2 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test1,PROD)");
  
  @Test
  private void testAddGlossaryTermToEntityExistingGlossaryTerm() throws Exception {
    GlossaryTerms existingGlossaryTerms = new GlossaryTerms();
    existingGlossaryTerms.setTerms(new GlossaryTermAssociationArray(ImmutableList.of(
        new GlossaryTermAssociation()
          .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_1))
    )));
    EntityClient mockClient = createMockGlossaryEntityClient(existingGlossaryTerms);

    final GlossaryTermService service = new GlossaryTermService(
        mockClient,
        Mockito.mock(Authentication.class));

    Urn newGlossaryTermUrn = UrnUtils.getUrn("urn:li:glossaryTerm:newGlossaryTerm");
    List<MetadataChangeProposal> events = service.buildAddGlossaryTermsProposals(
        ImmutableList.of(newGlossaryTermUrn),
        ImmutableList.of(
          new ResourceReference(TEST_ENTITY_URN_1, null, null),
          new ResourceReference(TEST_ENTITY_URN_2, null, null)),
        mockAuthentication());

    GlossaryTermAssociationArray expected = new GlossaryTermAssociationArray(
        ImmutableList.of(
            new GlossaryTermAssociation().setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_1)),
            new GlossaryTermAssociation().setUrn(GlossaryTermUrn.createFromUrn(newGlossaryTermUrn))));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.GLOSSARY_TERMS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    GlossaryTerms glossaryTermsAspect1 = GenericRecordUtils.deserializeAspect(
        event1.getAspect().getValue(),
        event1.getAspect().getContentType(),
        GlossaryTerms.class);
    Assert.assertEquals(glossaryTermsAspect1.getTerms(), expected);

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.GLOSSARY_TERMS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    GlossaryTerms glossaryTermsAspect2 = GenericRecordUtils.deserializeAspect(
        event2.getAspect().getValue(),
        event2.getAspect().getContentType(),
        GlossaryTerms.class);
    Assert.assertEquals(glossaryTermsAspect2.getTerms(), expected);
  }

  @Test
  private void testAddGlossaryTermsToEntityNoExistingGlossaryTerm() throws Exception {
    EntityClient mockClient = createMockGlossaryEntityClient(null);

    final GlossaryTermService service = new GlossaryTermService(
        mockClient,
        Mockito.mock(Authentication.class));

    Urn newGlossaryTermUrn = UrnUtils.getUrn("urn:li:glossaryTerm:newGlossaryTerm");
    List<MetadataChangeProposal> events = service.buildAddGlossaryTermsProposals(
        ImmutableList.of(newGlossaryTermUrn),
        ImmutableList.of(
          new ResourceReference(TEST_ENTITY_URN_1, null, null),
          new ResourceReference(TEST_ENTITY_URN_2, null, null)),
        mockAuthentication());

    GlossaryTermAssociationArray expectedTermsArray = new GlossaryTermAssociationArray(
        ImmutableList.of(new GlossaryTermAssociation().setUrn(GlossaryTermUrn.createFromUrn(newGlossaryTermUrn))));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.GLOSSARY_TERMS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    GlossaryTerms glossaryTermsAspect1 = GenericRecordUtils.deserializeAspect(
        event1.getAspect().getValue(),
        event1.getAspect().getContentType(),
        GlossaryTerms.class);
    Assert.assertEquals(glossaryTermsAspect1.getTerms(), expectedTermsArray);

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.GLOSSARY_TERMS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    GlossaryTerms glossaryTermsAspect2 = GenericRecordUtils.deserializeAspect(
        event2.getAspect().getValue(),
        event2.getAspect().getContentType(),
        GlossaryTerms.class);
    Assert.assertEquals(glossaryTermsAspect2.getTerms(), expectedTermsArray);
  }

  @Test
  private void testAddGlossaryTermToSchemaFieldExistingGlossaryTerm() throws Exception {
    EditableSchemaMetadata existingMetadata = new EditableSchemaMetadata();
    existingMetadata.setEditableSchemaFieldInfo(
        new EditableSchemaFieldInfoArray(ImmutableList.of(
            new EditableSchemaFieldInfo()
              .setFieldPath("myfield")
              .setGlossaryTerms(new GlossaryTerms().setTerms(new GlossaryTermAssociationArray(
                  ImmutableList.of(new GlossaryTermAssociation().setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_1)))
              )))
        ))
    );
    EntityClient mockClient = createMockSchemaMetadataEntityClient(existingMetadata);

    final GlossaryTermService service = new GlossaryTermService(
        mockClient,
        Mockito.mock(Authentication.class));

    Urn newGlossaryTermUrn = UrnUtils.getUrn("urn:li:glossaryTerm:newGlossaryTerm");
    List<MetadataChangeProposal> events = service.buildAddGlossaryTermsProposals(
        ImmutableList.of(newGlossaryTermUrn),
        ImmutableList.of(
            new ResourceReference(TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "myfield"),
            new ResourceReference(TEST_ENTITY_URN_2, SubResourceType.DATASET_FIELD, "myfield")),
        mockAuthentication());

    GlossaryTermAssociationArray expected = new GlossaryTermAssociationArray(
        ImmutableList.of(
            new GlossaryTermAssociation().setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_1)),
            new GlossaryTermAssociation().setUrn(GlossaryTermUrn.createFromUrn(newGlossaryTermUrn))));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    EditableSchemaMetadata editableSchemaMetadataAspect1 = GenericRecordUtils.deserializeAspect(
        event1.getAspect().getValue(),
        event1.getAspect().getContentType(),
        EditableSchemaMetadata.class);
    Assert.assertEquals(editableSchemaMetadataAspect1.getEditableSchemaFieldInfo().get(0).getGlossaryTerms().getTerms(), expected);

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    EditableSchemaMetadata editableSchemaMetadataAspect2 = GenericRecordUtils.deserializeAspect(
        event2.getAspect().getValue(),
        event2.getAspect().getContentType(),
        EditableSchemaMetadata.class);
    Assert.assertEquals(editableSchemaMetadataAspect2.getEditableSchemaFieldInfo().get(0).getGlossaryTerms().getTerms(), expected);
  }

  @Test
  private void testAddGlossaryTermsToSchemaFieldNoExistingGlossaryTerm() throws Exception {

    EditableSchemaMetadata existingMetadata = new EditableSchemaMetadata();
    existingMetadata.setEditableSchemaFieldInfo(
        new EditableSchemaFieldInfoArray(ImmutableList.of(
            new EditableSchemaFieldInfo()
                .setFieldPath("myfield")
                .setGlossaryTerms(new GlossaryTerms())))
    );

    EntityClient mockClient = createMockSchemaMetadataEntityClient(existingMetadata);

    final GlossaryTermService service = new GlossaryTermService(
        mockClient,
        Mockito.mock(Authentication.class));

    Urn newGlossaryTermUrn = UrnUtils.getUrn("urn:li:glossaryTerm:newGlossaryTerm");
    List<MetadataChangeProposal> events = service.buildAddGlossaryTermsProposals(
        ImmutableList.of(newGlossaryTermUrn),
        ImmutableList.of(
            new ResourceReference(TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "myfield"),
            new ResourceReference(TEST_ENTITY_URN_2, SubResourceType.DATASET_FIELD, "myfield")),
        mockAuthentication());

    GlossaryTermAssociationArray expected = new GlossaryTermAssociationArray(ImmutableList.of(
      new GlossaryTermAssociation().setUrn(GlossaryTermUrn.createFromUrn(newGlossaryTermUrn)))
    );

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    EditableSchemaMetadata editableSchemaMetadataAspect1 = GenericRecordUtils.deserializeAspect(
        event1.getAspect().getValue(),
        event1.getAspect().getContentType(),
        EditableSchemaMetadata.class);
    Assert.assertEquals(editableSchemaMetadataAspect1.getEditableSchemaFieldInfo().get(0).getGlossaryTerms().getTerms(), expected);

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    EditableSchemaMetadata editableSchemaMetadataAspect2 = GenericRecordUtils.deserializeAspect(
        event2.getAspect().getValue(),
        event2.getAspect().getContentType(),
        EditableSchemaMetadata.class);
    Assert.assertEquals(editableSchemaMetadataAspect2.getEditableSchemaFieldInfo().get(0).getGlossaryTerms().getTerms(), expected);
  }

  @Test
  private void testRemoveGlossaryTermToEntityExistingGlossaryTerm() throws Exception {
    GlossaryTerms existingGlossaryTerms = new GlossaryTerms();
    existingGlossaryTerms.setTerms(new GlossaryTermAssociationArray(ImmutableList.of(
        new GlossaryTermAssociation()
            .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_1)),
        new GlossaryTermAssociation()
            .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_2))
    )));
    EntityClient mockClient = createMockGlossaryEntityClient(existingGlossaryTerms);

    final GlossaryTermService service = new GlossaryTermService(
        mockClient,
        Mockito.mock(Authentication.class));

    List<MetadataChangeProposal> events = service.buildRemoveGlossaryTermsProposals(
        ImmutableList.of(TEST_GLOSSARY_TERM_URN_1),
        ImmutableList.of(
            new ResourceReference(TEST_ENTITY_URN_1, null, null),
            new ResourceReference(TEST_ENTITY_URN_2, null, null)),
        mockAuthentication());

    GlossaryTerms expected = new GlossaryTerms().setTerms(new GlossaryTermAssociationArray(
        ImmutableList.of(new GlossaryTermAssociation().setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_2)))));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.GLOSSARY_TERMS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate glossaryTermsAspect1 = GenericRecordUtils.deserializeAspect(
        event1.getAspect().getValue(),
        event1.getAspect().getContentType(),
        GlossaryTerms.class);
    Assert.assertEquals(glossaryTermsAspect1, expected);

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.GLOSSARY_TERMS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate glossaryTermsAspect2 = GenericRecordUtils.deserializeAspect(
        event2.getAspect().getValue(),
        event2.getAspect().getContentType(),
        GlossaryTerms.class);
    Assert.assertEquals(glossaryTermsAspect2, expected);
  }

  @Test
  private void testRemoveGlossaryTermsToEntityNoExistingGlossaryTerm() throws Exception {
    EntityClient mockClient = createMockGlossaryEntityClient(null);

    final GlossaryTermService service = new GlossaryTermService(
        mockClient,
        Mockito.mock(Authentication.class));

    Urn newGlossaryTermUrn = UrnUtils.getUrn("urn:li:glossaryTerm:newGlossaryTerm");
    List<MetadataChangeProposal> events = service.buildRemoveGlossaryTermsProposals(
        ImmutableList.of(newGlossaryTermUrn),
        ImmutableList.of(
            new ResourceReference(TEST_ENTITY_URN_1, null, null),
            new ResourceReference(TEST_ENTITY_URN_2, null, null)),
        mockAuthentication());

    GlossaryTermAssociationArray expected = new GlossaryTermAssociationArray(ImmutableList.of());

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.GLOSSARY_TERMS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    GlossaryTerms glossaryTermsAspect1 = GenericRecordUtils.deserializeAspect(
        event1.getAspect().getValue(),
        event1.getAspect().getContentType(),
        GlossaryTerms.class);
    Assert.assertEquals(glossaryTermsAspect1.getTerms(), expected);

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.GLOSSARY_TERMS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    GlossaryTerms glossaryTermsAspect2 = GenericRecordUtils.deserializeAspect(
        event2.getAspect().getValue(),
        event2.getAspect().getContentType(),
        GlossaryTerms.class);
    Assert.assertEquals(glossaryTermsAspect2.getTerms(), expected);
  }

  @Test
  private void testRemoveGlossaryTermToSchemaFieldExistingGlossaryTerm() throws Exception {
    EditableSchemaMetadata existingMetadata = new EditableSchemaMetadata();
    existingMetadata.setEditableSchemaFieldInfo(
        new EditableSchemaFieldInfoArray(ImmutableList.of(
            new EditableSchemaFieldInfo()
                .setFieldPath("myfield")
                .setGlossaryTerms(new GlossaryTerms().setTerms(new GlossaryTermAssociationArray(
                    ImmutableList.of(
                        new GlossaryTermAssociation().setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_1)),
                        new GlossaryTermAssociation().setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_2)))
                )))
        ))
    );
    EntityClient mockClient = createMockSchemaMetadataEntityClient(existingMetadata);

    final GlossaryTermService service = new GlossaryTermService(
        mockClient,
        Mockito.mock(Authentication.class));

    List<MetadataChangeProposal> events = service.buildRemoveGlossaryTermsProposals(
        ImmutableList.of(TEST_GLOSSARY_TERM_URN_1),
        ImmutableList.of(
            new ResourceReference(TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "myfield"),
            new ResourceReference(TEST_ENTITY_URN_2, SubResourceType.DATASET_FIELD, "myfield")),
        mockAuthentication());

    GlossaryTermAssociationArray expected = new GlossaryTermAssociationArray(ImmutableList.of(
        new GlossaryTermAssociation()
            .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_2))
    ));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    EditableSchemaMetadata editableSchemaMetadataAspect1 = GenericRecordUtils.deserializeAspect(
        event1.getAspect().getValue(),
        event1.getAspect().getContentType(),
        EditableSchemaMetadata.class);
    Assert.assertEquals(editableSchemaMetadataAspect1.getEditableSchemaFieldInfo().get(0).getGlossaryTerms().getTerms(), expected);

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    EditableSchemaMetadata editableSchemaMetadataAspect2 = GenericRecordUtils.deserializeAspect(
        event2.getAspect().getValue(),
        event2.getAspect().getContentType(),
        EditableSchemaMetadata.class);
    Assert.assertEquals(editableSchemaMetadataAspect2.getEditableSchemaFieldInfo().get(0).getGlossaryTerms().getTerms(), expected);
  }

  @Test
  private void testRemoveGlossaryTermsToSchemaFieldNoExistingGlossaryTerm() throws Exception {

    EditableSchemaMetadata existingMetadata = new EditableSchemaMetadata();
    existingMetadata.setEditableSchemaFieldInfo(
        new EditableSchemaFieldInfoArray(ImmutableList.of(
            new EditableSchemaFieldInfo()
                .setFieldPath("myfield")
                .setGlossaryTerms(new GlossaryTerms())))
    );

    EntityClient mockClient = createMockSchemaMetadataEntityClient(existingMetadata);

    final GlossaryTermService service = new GlossaryTermService(
        mockClient,
        Mockito.mock(Authentication.class));

    List<MetadataChangeProposal> events = service.buildRemoveGlossaryTermsProposals(
        ImmutableList.of(TEST_ENTITY_URN_1),
        ImmutableList.of(
            new ResourceReference(TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "myfield"),
            new ResourceReference(TEST_ENTITY_URN_2, SubResourceType.DATASET_FIELD, "myfield")),
        mockAuthentication());

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    EditableSchemaMetadata editableSchemaMetadataAspect1 = GenericRecordUtils.deserializeAspect(
        event1.getAspect().getValue(),
        event1.getAspect().getContentType(),
        EditableSchemaMetadata.class);
    Assert.assertEquals(editableSchemaMetadataAspect1.getEditableSchemaFieldInfo().get(0).getGlossaryTerms().getTerms(), Collections.emptyList());

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    EditableSchemaMetadata editableSchemaMetadataAspect2 = GenericRecordUtils.deserializeAspect(
        event2.getAspect().getValue(),
        event2.getAspect().getContentType(),
        EditableSchemaMetadata.class);
    Assert.assertEquals(editableSchemaMetadataAspect2.getEditableSchemaFieldInfo().get(0).getGlossaryTerms().getTerms(), Collections.emptyList());

  }

  private static EntityClient createMockGlossaryEntityClient(@Nullable GlossaryTerms existingGlossaryTerms) throws Exception {
    return createMockEntityClient(existingGlossaryTerms, Constants.GLOSSARY_TERMS_ASPECT_NAME);
  }

  private static EntityClient createMockSchemaMetadataEntityClient(@Nullable EditableSchemaMetadata existingMetadata) throws Exception {
    return createMockEntityClient(existingMetadata, Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
  }

  private static EntityClient createMockEntityClient(@Nullable RecordTemplate aspect, String aspectName) throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(mockClient.batchGetV2(
        Mockito.eq(Constants.DATASET_ENTITY_NAME),
        Mockito.eq(ImmutableSet.of(TEST_ENTITY_URN_1, TEST_ENTITY_URN_2)),
        Mockito.eq(ImmutableSet.of(aspectName)),
        Mockito.any(Authentication.class)))
        .thenReturn(aspect != null ? ImmutableMap.of(
            TEST_ENTITY_URN_1,
            new EntityResponse()
                .setUrn(TEST_ENTITY_URN_1)
                .setEntityName(Constants.DATASET_ENTITY_NAME)
                .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
                    aspectName,
                    new EnvelopedAspect().setValue(new Aspect(aspect.data()))
                ))),
            TEST_ENTITY_URN_2,
            new EntityResponse()
                .setUrn(TEST_ENTITY_URN_2)
                .setEntityName(Constants.DATASET_ENTITY_NAME)
                .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
                    aspectName,
                    new EnvelopedAspect().setValue(new Aspect(aspect.data()))
                )))
        ) : Collections.emptyMap());
    return mockClient;
  }

  private static Authentication mockAuthentication() {
    Authentication mockAuth = Mockito.mock(Authentication.class);
    Mockito.when(mockAuth.getActor()).thenReturn(new Actor(ActorType.USER, Constants.SYSTEM_ACTOR));
    return mockAuth;
  }
}
