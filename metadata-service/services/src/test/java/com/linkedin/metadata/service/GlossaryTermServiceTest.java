package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERMS_ASPECT_NAME;
import static com.linkedin.metadata.service.util.MCPUtils.TestMCP;
import static com.linkedin.metadata.service.util.ServiceTestUtils.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
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
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.resource.SubResourceType;
import com.linkedin.metadata.service.util.MCPUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GlossaryTermServiceTest {

  private static final Urn TEST_GLOSSARY_TERM_URN_1 = UrnUtils.getUrn("urn:li:glossaryTerm:test");
  private static final Urn TEST_GLOSSARY_TERM_URN_2 = UrnUtils.getUrn("urn:li:glossaryTerm:test2");

  private static final long TEST_TIME = 123456789L;
  private static final long OLD_TEST_TIME = 5L;
  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final Urn OLD_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:old");

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

  private static AspectRetriever mockAspectRetriever;
  private static OperationContext opContext;
  private static MockedStatic<GlossaryTermService> staticMock;
  private static GlossaryTermService service;

  @BeforeClass
  public void init() {
    mockAspectRetriever = mock(AspectRetriever.class);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization(mockAspectRetriever);
    staticMock = Mockito.mockStatic(GlossaryTermService.class, CALLS_REAL_METHODS);
    staticMock.when(GlossaryTermService::getTimestamp).thenReturn(TEST_TIME);
    OpenApiClient mockOpenAPIClient = createMockClient(); // Unused for now...
    service =
        new GlossaryTermService(
            mock(SystemEntityClient.class), mockOpenAPIClient, opContext.getObjectMapper());
  }

  @BeforeMethod
  public void setup() {
    reset(mockAspectRetriever);
  }

  @AfterClass
  public static void tearDown() {
    if (staticMock != null) {
      staticMock.close();
    }
  }

  @Test
  public void testAddGlossaryTermToEntityExistingGlossaryTerm() throws Exception {
    GlossaryTerms existingGlossaryTerms = new GlossaryTerms();
    existingGlossaryTerms.setTerms(
        new GlossaryTermAssociationArray(
            ImmutableList.of(
                new GlossaryTermAssociation()
                    .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_1)))));

    mockGlossaryService(existingGlossaryTerms, existingGlossaryTerms);

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
  public void testAddGlossaryTermsToEntityNoExistingGlossaryTerm() throws Exception {
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
  public void testAddGlossaryTermToSchemaFieldExistingGlossaryTerm() throws Exception {
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

    mockGlossaryService(existingMetadata, existingMetadata);

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
  public void testAddGlossaryTermsToSchemaFieldNoExistingGlossaryTerm() throws Exception {

    EditableSchemaMetadata existingMetadata = new EditableSchemaMetadata();
    existingMetadata.setEditableSchemaFieldInfo(
        new EditableSchemaFieldInfoArray(
            ImmutableList.of(
                new EditableSchemaFieldInfo()
                    .setFieldPath("myfield")
                    .setGlossaryTerms(new GlossaryTerms()))));

    mockGlossaryService(existingMetadata, existingMetadata);

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
  public void testRemoveGlossaryTermToEntityExistingGlossaryTerm() throws Exception {
    GlossaryTerms hasBoth =
        new GlossaryTerms()
            .setAuditStamp(new AuditStamp().setActor(OLD_ACTOR_URN).setTime(OLD_TEST_TIME))
            .setTerms(
                new GlossaryTermAssociationArray(
                    List.of(
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_1))
                            .setActor(OLD_ACTOR_URN),
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_2))
                            .setActor(OLD_ACTOR_URN))));

    GlossaryTerms hasOnlyTwo =
        new GlossaryTerms()
            .setAuditStamp(new AuditStamp().setActor(OLD_ACTOR_URN).setTime(OLD_TEST_TIME))
            .setTerms(
                new GlossaryTermAssociationArray(
                    List.of(
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_2))
                            .setActor(OLD_ACTOR_URN))));

    mockGlossaryService(hasBoth, hasOnlyTwo);

    List<MetadataChangeProposal> events =
        service.buildRemoveGlossaryTermsProposals(
            opContext,
            List.of(TEST_GLOSSARY_TERM_URN_1),
            List.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            TEST_ACTOR_URN);

    GlossaryTerms expectedChanged =
        new GlossaryTerms()
            .setAuditStamp(new AuditStamp().setActor(TEST_ACTOR_URN).setTime(TEST_TIME))
            .setTerms(
                new GlossaryTermAssociationArray(
                    List.of(
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_2))
                            .setActor(OLD_ACTOR_URN))));

    MCPUtils.assertEquals(
        events,
        List.of(
            new TestMCP(TEST_ENTITY_URN_1, expectedChanged),
            new TestMCP(TEST_ENTITY_URN_2, hasOnlyTwo)));
  }

  @Test
  public void testRemoveGlossaryTermsToEntityNoExistingGlossaryTerm() throws Exception {
    Urn newGlossaryTermUrn = UrnUtils.getUrn("urn:li:glossaryTerm:newGlossaryTerm");

    List<MetadataChangeProposal> events =
        service.buildRemoveGlossaryTermsProposals(
            opContext,
            List.of(newGlossaryTermUrn),
            List.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            TEST_ACTOR_URN);

    MCPUtils.assertEquals(events, List.of());
  }

  @Test
  public void testRemoveOnSchemaFieldWithGlossaryTermsAspect() throws Exception {
    GlossaryTerms gtHasBoth =
        new GlossaryTerms()
            .setAuditStamp(new AuditStamp().setActor(OLD_ACTOR_URN).setTime(OLD_TEST_TIME))
            .setTerms(
                new GlossaryTermAssociationArray(
                    List.of(
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_1))
                            .setActor(OLD_ACTOR_URN),
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_2))
                            .setActor(OLD_ACTOR_URN))));

    GlossaryTerms gtHasOnlyTwo =
        new GlossaryTerms()
            .setAuditStamp(new AuditStamp().setActor(OLD_ACTOR_URN).setTime(OLD_TEST_TIME))
            .setTerms(
                new GlossaryTermAssociationArray(
                    List.of(
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_2))
                            .setActor(OLD_ACTOR_URN))));

    EditableSchemaMetadata esmHasBoth =
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(
                new EditableSchemaFieldInfoArray(
                    List.of(
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH_1)
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(OLD_ACTOR_URN)
                                            .setTime(OLD_TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(
                                            List.of(
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromUrn(
                                                            TEST_GLOSSARY_TERM_URN_1))
                                                    .setActor(OLD_ACTOR_URN),
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromUrn(
                                                            TEST_GLOSSARY_TERM_URN_2))
                                                    .setActor(OLD_ACTOR_URN))))),
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH_2)
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(OLD_ACTOR_URN)
                                            .setTime(OLD_TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(
                                            List.of(
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromUrn(
                                                            TEST_GLOSSARY_TERM_URN_1))
                                                    .setActor(OLD_ACTOR_URN),
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromUrn(
                                                            TEST_GLOSSARY_TERM_URN_2))
                                                    .setActor(OLD_ACTOR_URN))))))));
    EditableSchemaMetadata esmHasOnlyTwo =
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(
                new EditableSchemaFieldInfoArray(
                    List.of(
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH_1)
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(OLD_ACTOR_URN)
                                            .setTime(OLD_TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(
                                            List.of(
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromUrn(
                                                            TEST_GLOSSARY_TERM_URN_2))
                                                    .setActor(OLD_ACTOR_URN))))),
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH_2)
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(OLD_ACTOR_URN)
                                            .setTime(OLD_TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(
                                            List.of(
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromUrn(
                                                            TEST_GLOSSARY_TERM_URN_2))
                                                    .setActor(OLD_ACTOR_URN))))))));

    mockGlossaryService(esmHasBoth, esmHasOnlyTwo, gtHasBoth, gtHasOnlyTwo);

    List<MetadataChangeProposal> events =
        service.buildRemoveGlossaryTermsProposals(
            opContext,
            List.of(TEST_GLOSSARY_TERM_URN_1),
            List.of(
                new ResourceReference(
                    TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, TEST_FIELD_PATH_1),
                new ResourceReference(
                    TEST_ENTITY_URN_2, SubResourceType.DATASET_FIELD, TEST_FIELD_PATH_2)),
            TEST_ACTOR_URN);

    GlossaryTerms gtExpectedChanged =
        new GlossaryTerms()
            .setAuditStamp(new AuditStamp().setActor(TEST_ACTOR_URN).setTime(TEST_TIME))
            .setTerms(
                new GlossaryTermAssociationArray(
                    List.of(
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_2))
                            .setActor(OLD_ACTOR_URN))));

    EditableSchemaMetadata esmExpectedChange =
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(
                new EditableSchemaFieldInfoArray(
                    List.of(
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH_1)
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(TEST_ACTOR_URN)
                                            .setTime(TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(
                                            List.of(
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromUrn(
                                                            TEST_GLOSSARY_TERM_URN_2))
                                                    .setActor(OLD_ACTOR_URN))))),
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH_2)
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(OLD_ACTOR_URN)
                                            .setTime(OLD_TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(
                                            List.of(
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromUrn(
                                                            TEST_GLOSSARY_TERM_URN_1))
                                                    .setActor(OLD_ACTOR_URN),
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromUrn(
                                                            TEST_GLOSSARY_TERM_URN_2))
                                                    .setActor(OLD_ACTOR_URN))))))));

    MCPUtils.assertEquals(
        events,
        List.of(
            new TestMCP(TEST_SCHEMA_FIELD_URN_1, gtExpectedChanged),
            new TestMCP(TEST_SCHEMA_FIELD_URN_2, gtHasOnlyTwo),
            new TestMCP(TEST_ENTITY_URN_1, esmExpectedChange),
            new TestMCP(TEST_ENTITY_URN_2, esmHasOnlyTwo)));
  }

  @Test
  public void testRemoveOnSchemaFieldUrn() throws Exception {
    GlossaryTerms gtHasBoth =
        new GlossaryTerms()
            .setAuditStamp(new AuditStamp().setActor(OLD_ACTOR_URN).setTime(OLD_TEST_TIME))
            .setTerms(
                new GlossaryTermAssociationArray(
                    List.of(
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_1))
                            .setActor(OLD_ACTOR_URN),
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_2))
                            .setActor(OLD_ACTOR_URN))));

    GlossaryTerms gtHasOnlyTwo =
        new GlossaryTerms()
            .setAuditStamp(new AuditStamp().setActor(OLD_ACTOR_URN).setTime(OLD_TEST_TIME))
            .setTerms(
                new GlossaryTermAssociationArray(
                    List.of(
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_2))
                            .setActor(OLD_ACTOR_URN))));

    EditableSchemaMetadata esmHasBoth =
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(
                new EditableSchemaFieldInfoArray(
                    List.of(
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH_1)
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(OLD_ACTOR_URN)
                                            .setTime(OLD_TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(
                                            List.of(
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromUrn(
                                                            TEST_GLOSSARY_TERM_URN_1))
                                                    .setActor(OLD_ACTOR_URN),
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromUrn(
                                                            TEST_GLOSSARY_TERM_URN_2))
                                                    .setActor(OLD_ACTOR_URN))))),
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH_2)
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(OLD_ACTOR_URN)
                                            .setTime(OLD_TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(
                                            List.of(
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromUrn(
                                                            TEST_GLOSSARY_TERM_URN_1))
                                                    .setActor(OLD_ACTOR_URN),
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromUrn(
                                                            TEST_GLOSSARY_TERM_URN_2))
                                                    .setActor(OLD_ACTOR_URN))))))));
    EditableSchemaMetadata esmHasOnlyTwo =
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(
                new EditableSchemaFieldInfoArray(
                    List.of(
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH_1)
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(OLD_ACTOR_URN)
                                            .setTime(OLD_TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(
                                            List.of(
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromUrn(
                                                            TEST_GLOSSARY_TERM_URN_2))
                                                    .setActor(OLD_ACTOR_URN))))),
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH_2)
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(OLD_ACTOR_URN)
                                            .setTime(OLD_TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(
                                            List.of(
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromUrn(
                                                            TEST_GLOSSARY_TERM_URN_2))
                                                    .setActor(OLD_ACTOR_URN))))))));

    mockGlossaryService(esmHasBoth, esmHasOnlyTwo, gtHasBoth, gtHasOnlyTwo);

    List<MetadataChangeProposal> events =
        service.buildRemoveGlossaryTermsProposals(
            opContext,
            List.of(TEST_GLOSSARY_TERM_URN_1),
            List.of(
                new ResourceReference(TEST_SCHEMA_FIELD_URN_1, null, null),
                new ResourceReference(TEST_SCHEMA_FIELD_URN_2, null, null)),
            TEST_ACTOR_URN);

    GlossaryTerms gtExpectedChanged =
        new GlossaryTerms()
            .setAuditStamp(new AuditStamp().setActor(TEST_ACTOR_URN).setTime(TEST_TIME))
            .setTerms(
                new GlossaryTermAssociationArray(
                    List.of(
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromUrn(TEST_GLOSSARY_TERM_URN_2))
                            .setActor(OLD_ACTOR_URN))));

    EditableSchemaMetadata esmExpectedChange =
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(
                new EditableSchemaFieldInfoArray(
                    List.of(
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH_1)
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(TEST_ACTOR_URN)
                                            .setTime(TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(
                                            List.of(
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromUrn(
                                                            TEST_GLOSSARY_TERM_URN_2))
                                                    .setActor(OLD_ACTOR_URN))))),
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH_2)
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(OLD_ACTOR_URN)
                                            .setTime(OLD_TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(
                                            List.of(
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromUrn(
                                                            TEST_GLOSSARY_TERM_URN_1))
                                                    .setActor(OLD_ACTOR_URN),
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromUrn(
                                                            TEST_GLOSSARY_TERM_URN_2))
                                                    .setActor(OLD_ACTOR_URN))))))));

    MCPUtils.assertEquals(
        events,
        List.of(
            new TestMCP(TEST_SCHEMA_FIELD_URN_1, gtExpectedChanged),
            new TestMCP(TEST_SCHEMA_FIELD_URN_2, gtHasOnlyTwo),
            new TestMCP(TEST_ENTITY_URN_1, esmExpectedChange),
            new TestMCP(TEST_ENTITY_URN_2, esmHasOnlyTwo)));
  }

  @Test
  public void testRemoveGlossaryTermsToSchemaFieldNoExistingGlossaryTerm() throws Exception {

    EditableSchemaMetadata existingMetadata = new EditableSchemaMetadata();
    existingMetadata.setEditableSchemaFieldInfo(
        new EditableSchemaFieldInfoArray(
            List.of(
                new EditableSchemaFieldInfo()
                    .setFieldPath("myfield")
                    .setGlossaryTerms(new GlossaryTerms()))));

    mockGlossaryService(existingMetadata, existingMetadata);

    List<MetadataChangeProposal> events =
        service.buildRemoveGlossaryTermsProposals(
            opContext,
            List.of(TEST_ENTITY_URN_1),
            List.of(
                new ResourceReference(TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "myfield"),
                new ResourceReference(TEST_ENTITY_URN_2, SubResourceType.DATASET_FIELD, "myfield")),
            TEST_ACTOR_URN);

    MCPUtils.assertEquals(events, List.of());
  }

  @Test
  public void testGetSchemaFieldTermsAll() throws Exception {
    final Urn termUrn1 = UrnUtils.getUrn("urn:li:glossaryTerm:existingTerm1");
    final Urn termUrn2 = UrnUtils.getUrn("urn:li:glossaryTerm:existingTerm2");
    final Urn termUrn3 = UrnUtils.getUrn("urn:li:glossaryTerm:existingTerm3");

    final EditableSchemaMetadata existingEditableSchemaMetadata = new EditableSchemaMetadata();
    existingEditableSchemaMetadata.setEditableSchemaFieldInfo(
        new EditableSchemaFieldInfoArray(
            ImmutableList.of(
                new EditableSchemaFieldInfo()
                    .setFieldPath(TEST_FIELD_PATH_1)
                    .setGlossaryTerms(
                        new GlossaryTerms()
                            .setTerms(
                                new GlossaryTermAssociationArray(
                                    List.of(
                                        new GlossaryTermAssociation()
                                            .setUrn(GlossaryTermUrn.createFromUrn(termUrn1)))))))));

    final SchemaMetadata existingSchemaMetadata = new SchemaMetadata();
    existingSchemaMetadata.setFields(
        new SchemaFieldArray(
            ImmutableList.of(
                new SchemaField()
                    .setFieldPath(TEST_FIELD_PATH_1)
                    .setGlossaryTerms(
                        new GlossaryTerms()
                            .setTerms(
                                new GlossaryTermAssociationArray(
                                    List.of(
                                        new GlossaryTermAssociation()
                                            .setUrn(GlossaryTermUrn.createFromUrn(termUrn2)))))))));
    final GlossaryTerms existingTerms =
        new GlossaryTerms()
            .setTerms(
                new GlossaryTermAssociationArray(
                    List.of(
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromUrn(termUrn3)))));

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
                Mockito.eq(ImmutableSet.of(GLOSSARY_TERMS_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.GLOSSARY_TERMS_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(existingTerms.data()))))));

    final List<GlossaryTermAssociation> terms =
        service.getSchemaFieldTerms(opContext, TEST_ENTITY_URN_1, TEST_FIELD_PATH_1);

    Assert.assertEquals(terms.size(), 3);
    Assert.assertEquals(terms.get(0).getUrn().toString(), termUrn3.toString());
    Assert.assertEquals(terms.get(1).getUrn().toString(), termUrn1.toString());
    Assert.assertEquals(terms.get(2).getUrn().toString(), termUrn2.toString());
  }

  @Test
  public void testGetSchemaFieldTermsNoTerms() throws Exception {
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
                Mockito.eq(ImmutableSet.of(GLOSSARY_TERMS_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse().setAspects(new EnvelopedAspectMap(Collections.emptyMap())));

    final List<GlossaryTermAssociation> terms =
        service.getSchemaFieldTerms(opContext, TEST_ENTITY_URN_1, TEST_FIELD_PATH_1);

    Assert.assertEquals(terms.size(), 0);
  }

  public void mockGlossaryService(
      @Nullable EditableSchemaMetadata existingSM1, @Nullable EditableSchemaMetadata existingSM2) {
    setMockAspectRetriever(
        EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
        TEST_ENTITY_URN_1,
        TEST_ENTITY_URN_2,
        existingSM1,
        existingSM2);
  }

  public void mockGlossaryService(
      @Nullable GlossaryTerms existingGT1, @Nullable GlossaryTerms existingGT2) {
    setMockAspectRetriever(
        GLOSSARY_TERMS_ASPECT_NAME, TEST_ENTITY_URN_1, TEST_ENTITY_URN_2, existingGT1, existingGT2);
  }

  public void mockGlossaryService(
      @Nullable EditableSchemaMetadata existingSM1,
      @Nullable EditableSchemaMetadata existingSM2,
      @Nullable GlossaryTerms existingGT1,
      @Nullable GlossaryTerms existingGT2) {
    setMockAspectRetriever(
        EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
        TEST_ENTITY_URN_1,
        TEST_ENTITY_URN_2,
        existingSM1,
        existingSM2);
    setMockAspectRetriever(
        GLOSSARY_TERMS_ASPECT_NAME,
        TEST_SCHEMA_FIELD_URN_1,
        TEST_SCHEMA_FIELD_URN_2,
        existingGT1,
        existingGT2);
  }

  private static void setMockAspectRetriever(
      String aspectName,
      Urn urn1,
      Urn urn2,
      @Nullable RecordTemplate existingGlossaryTerms1,
      @Nullable RecordTemplate existingGlossaryTerms2) {
    Map<Urn, Map<String, Aspect>> map1 =
        existingGlossaryTerms1 != null
            ? Map.of(urn1, Map.of(aspectName, new Aspect(existingGlossaryTerms1.data())))
            : Map.of();
    Map<Urn, Map<String, Aspect>> map2 =
        existingGlossaryTerms2 != null
            ? Map.of(urn2, Map.of(aspectName, new Aspect(existingGlossaryTerms2.data())))
            : Map.of();

    Mockito.when(
            mockAspectRetriever.getLatestAspectObjects(
                eq(Set.of(urn1, urn2)), eq(Set.of(aspectName))))
        .thenReturn(
            // Merge maps
            Stream.of(map1, map2)
                .flatMap(m -> m.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
  }
}
