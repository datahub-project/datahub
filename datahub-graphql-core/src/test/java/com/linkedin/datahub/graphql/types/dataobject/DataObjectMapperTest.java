package com.linkedin.datahub.graphql.types.dataobject;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.application.Applications;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Deprecation;
import com.linkedin.common.Documentation;
import com.linkedin.common.DocumentationAssociation;
import com.linkedin.common.DocumentationAssociationArray;
import com.linkedin.common.Forms;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.Status;
import com.linkedin.common.SubTypes;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.DataObject;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.dataobject.DataObjectProperties;
import com.linkedin.dataobject.ObjectStorageProperties;
import com.linkedin.dataobject.ParentDataObject;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.DataObjectKey;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.mockito.MockedStatic;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataObjectMapperTest {

  private static final String TEST_DATA_OBJECT_URN =
      "urn:li:dataObject:(urn:li:dataPlatform:s3,b/clip.mp4,PROD)";
  private static final String TEST_PLATFORM_URN = "urn:li:dataPlatform:s3";
  private static final String TEST_PARENT_URN =
      "urn:li:dataObject:(urn:li:dataPlatform:s3,b/parent,PROD)";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:testuser";
  private static final Long TEST_TIMESTAMP = 1640995200000L;

  private Urn dataObjectUrn;
  private Urn actorUrn;
  private QueryContext mockQueryContext;

  @BeforeMethod
  public void setup() throws URISyntaxException {
    dataObjectUrn = Urn.createFromString(TEST_DATA_OBJECT_URN);
    actorUrn = Urn.createFromString(TEST_ACTOR_URN);
    mockQueryContext = mock(QueryContext.class);
  }

  @Test
  public void testMapDataObjectProperties() {
    EntityResponse response = createBasicEntityResponse();

    DataObjectProperties properties = new DataObjectProperties();
    properties.setName("clip.mp4");
    properties.setQualifiedName("b/clip.mp4");
    properties.setDescription("A video clip");
    properties.setExternalUrl(new Url("https://example.com/b/clip.mp4"));
    StringMap customProperties = new StringMap();
    customProperties.put("region", "us-east-1");
    properties.setCustomProperties(customProperties);
    properties.setCreated(new AuditStamp().setTime(TEST_TIMESTAMP).setActor(actorUrn));
    properties.setLastModified(new AuditStamp().setTime(TEST_TIMESTAMP).setActor(actorUrn));
    addAspectToResponse(response, DATA_OBJECT_PROPERTIES_ASPECT_NAME, properties);

    ObjectStorageProperties storage = new ObjectStorageProperties();
    storage.setMimeType("video/mp4");
    storage.setSizeBytes(1024L);
    storage.setStorageClass("STANDARD");
    addAspectToResponse(response, OBJECT_STORAGE_PROPERTIES_ASPECT_NAME, storage);

    DataObject result = DataObjectMapper.map(null, response);

    assertEquals(result.getType(), EntityType.DATA_OBJECT);
    assertEquals(result.getUrn(), TEST_DATA_OBJECT_URN);
    assertEquals(result.getName(), "clip.mp4");

    assertNotNull(result.getProperties());
    assertEquals(result.getProperties().getName(), "clip.mp4");
    assertEquals(result.getProperties().getQualifiedName(), "b/clip.mp4");
    assertEquals(result.getProperties().getDescription(), "A video clip");
    assertEquals(result.getProperties().getExternalUrl(), "https://example.com/b/clip.mp4");
    assertNotNull(result.getProperties().getCustomProperties());
    assertEquals(result.getProperties().getCustomProperties().size(), 1);
    assertNotNull(result.getProperties().getCreated());
    assertEquals(result.getProperties().getCreated().getTime(), TEST_TIMESTAMP.longValue());
    assertEquals(result.getProperties().getCreated().getActor(), TEST_ACTOR_URN);

    assertNotNull(result.getStorage());
    assertEquals(result.getStorage().getMimeType(), "video/mp4");
    assertEquals(result.getStorage().getSizeBytes(), Long.valueOf(1024L));
    assertEquals(result.getStorage().getStorageClass(), "STANDARD");
  }

  @Test
  public void testMapDataObjectKeyPopulatesPlatform() {
    EntityResponse response = createBasicEntityResponse();

    DataObject result = DataObjectMapper.map(null, response);

    assertNotNull(result.getPlatform());
    assertEquals(result.getPlatform().getUrn(), TEST_PLATFORM_URN);
    assertEquals(result.getPlatform().getType(), EntityType.DATA_PLATFORM);
    // name comes from the key when no properties aspect is present
    assertEquals(result.getName(), "b/clip.mp4");
  }

  @Test
  public void testMapParentDataObjectStub() throws URISyntaxException {
    EntityResponse response = createBasicEntityResponse();

    ParentDataObject parent = new ParentDataObject();
    parent.setObject(Urn.createFromString(TEST_PARENT_URN));
    addAspectToResponse(response, PARENT_DATA_OBJECT_ASPECT_NAME, parent);

    DataObject result = DataObjectMapper.map(null, response);

    assertNotNull(result.getParent());
    assertEquals(result.getParent().getUrn(), TEST_PARENT_URN);
    assertEquals(result.getParent().getType(), EntityType.DATA_OBJECT);
  }

  @Test
  public void testMapStatusRemovedSetsExistsFalse() {
    EntityResponse response = createBasicEntityResponse();

    Status status = new Status();
    status.setRemoved(true);
    addAspectToResponse(response, STATUS_ASPECT_NAME, status);

    DataObject result = DataObjectMapper.map(null, response);

    assertNotNull(result.getExists());
    assertFalse(result.getExists());
  }

  @Test
  public void testMapPopulatesDeprecatedGlobalTagsAlias() {
    // The shared sidebar tags section reads the deprecated globalTags alias, so DataObject must
    // populate both tags and globalTags from the GlobalTags aspect (same pattern as Dataset).
    EntityResponse response = createBasicEntityResponse();

    GlobalTags globalTags = new GlobalTags();
    globalTags.setTags(
        new TagAssociationArray(
            com.google.common.collect.ImmutableList.of(
                new TagAssociation().setTag(new TagUrn("media")))));
    addAspectToResponse(response, GLOBAL_TAGS_ASPECT_NAME, globalTags);

    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock
          .when(() -> AuthorizationUtils.canView(any(), eq(dataObjectUrn)))
          .thenReturn(true);

      DataObject result = DataObjectMapper.map(mockQueryContext, response);

      assertNotNull(result.getTags());
      assertNotNull(result.getGlobalTags());
      assertSame(result.getGlobalTags(), result.getTags());
    }
  }

  @Test
  public void testMapWithRestrictedAccess() {
    EntityResponse response = createBasicEntityResponse();

    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock
          .when(() -> AuthorizationUtils.canView(any(), eq(dataObjectUrn)))
          .thenReturn(false);

      DataObject restricted = new DataObject();
      authUtilsMock
          .when(
              () -> AuthorizationUtils.restrictEntity(any(DataObject.class), eq(DataObject.class)))
          .thenReturn(restricted);

      DataObject result = DataObjectMapper.map(mockQueryContext, response);

      assertSame(result, restricted);
    }
  }

  @Test
  public void testMapDocumentationAspect() {
    EntityResponse response = createBasicEntityResponse();

    Documentation documentation = new Documentation();
    DocumentationAssociation association = new DocumentationAssociation();
    association.setDocumentation("A detailed description of this data object.");
    documentation.setDocumentations(new DocumentationAssociationArray(association));
    addAspectToResponse(response, DOCUMENTATION_ASPECT_NAME, documentation);

    DataObject result = DataObjectMapper.map(null, response);

    assertNotNull(result.getDocumentation());
    assertNotNull(result.getDocumentation().getDocumentations());
    assertFalse(result.getDocumentation().getDocumentations().isEmpty());
    assertEquals(
        result.getDocumentation().getDocumentations().get(0).getDocumentation(),
        "A detailed description of this data object.");
  }

  @Test
  public void testMapApplicationMembership() throws URISyntaxException {
    // The applications capability is declared on dataObject but was once fetched-and-dropped /
    // never mapped. This asserts the membership URN survives mapping end-to-end -- the assertion
    // that catches a regression of that gap.
    EntityResponse response = createBasicEntityResponse();

    final String applicationUrn = "urn:li:application:test-app";
    Applications applications = new Applications();
    applications.setApplications(new UrnArray(Urn.createFromString(applicationUrn)));
    addAspectToResponse(response, APPLICATION_MEMBERSHIP_ASPECT_NAME, applications);

    // context == null bypasses the per-URN canView filter in ApplicationAssociationMapper.
    DataObject result = DataObjectMapper.map(null, response);

    assertNotNull(result.getApplications());
    assertEquals(result.getApplications().size(), 1);
    assertEquals(result.getApplications().get(0).getApplication().getUrn(), applicationUrn);
    assertEquals(result.getApplications().get(0).getAssociatedUrn(), TEST_DATA_OBJECT_URN);
  }

  @Test
  public void testMapGovernanceAspects() throws URISyntaxException {
    EntityResponse response = createBasicEntityResponse();

    Ownership ownership = new Ownership();
    ownership.setOwners(
        new OwnerArray(new Owner().setType(OwnershipType.DATAOWNER).setOwner(actorUrn)));
    addAspectToResponse(response, OWNERSHIP_ASPECT_NAME, ownership);

    final Urn domainUrn = Urn.createFromString("urn:li:domain:marketing");
    Domains domains = new Domains();
    domains.setDomains(new UrnArray(domainUrn));
    domains.setDomainAssociations(
        new com.linkedin.domain.DomainAssociationArray(
            new com.linkedin.domain.DomainAssociation().setDomain(domainUrn)));
    addAspectToResponse(response, DOMAINS_ASPECT_NAME, domains);

    GlossaryTerms glossaryTerms = new GlossaryTerms();
    glossaryTerms.setTerms(
        new GlossaryTermAssociationArray(
            new GlossaryTermAssociation().setUrn(new GlossaryTermUrn("Classification.PII"))));
    addAspectToResponse(response, GLOSSARY_TERMS_ASPECT_NAME, glossaryTerms);

    Deprecation deprecation = new Deprecation();
    deprecation.setDeprecated(true);
    deprecation.setNote("retired");
    deprecation.setActor(actorUrn);
    addAspectToResponse(response, DEPRECATION_ASPECT_NAME, deprecation);

    Forms forms = new Forms();
    forms.setIncompleteForms(new com.linkedin.common.FormAssociationArray());
    forms.setCompletedForms(new com.linkedin.common.FormAssociationArray());
    addAspectToResponse(response, FORMS_ASPECT_NAME, forms);

    StructuredProperties structuredProperties = new StructuredProperties();
    structuredProperties.setProperties(new StructuredPropertyValueAssignmentArray());
    addAspectToResponse(response, STRUCTURED_PROPERTIES_ASPECT_NAME, structuredProperties);

    com.linkedin.container.Container container = new com.linkedin.container.Container();
    container.setContainer(Urn.createFromString("urn:li:container:bucket-1"));
    addAspectToResponse(response, CONTAINER_ASPECT_NAME, container);

    SubTypes subTypes = new SubTypes();
    subTypes.setTypeNames(new StringArray("File"));
    addAspectToResponse(response, SUB_TYPES_ASPECT_NAME, subTypes);

    DataObject result = DataObjectMapper.map(null, response);

    assertNotNull(result.getOwnership());
    assertEquals(result.getOwnership().getOwners().size(), 1);

    assertNotNull(result.getDomain());

    assertNotNull(result.getGlossaryTerms());
    assertEquals(result.getGlossaryTerms().getTerms().size(), 1);

    assertNotNull(result.getDeprecation());
    assertTrue(result.getDeprecation().getDeprecated());

    assertNotNull(result.getForms());

    assertNotNull(result.getStructuredProperties());

    assertNotNull(result.getContainer());
    assertEquals(result.getContainer().getUrn(), "urn:li:container:bucket-1");

    assertNotNull(result.getSubTypes());
    assertEquals(result.getSubTypes().getTypeNames().get(0), "File");
  }

  private EntityResponse createBasicEntityResponse() {
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(dataObjectUrn);

    DataObjectKey key = new DataObjectKey();
    try {
      key.setPlatform(Urn.createFromString(TEST_PLATFORM_URN));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    key.setName("b/clip.mp4");
    key.setOrigin(com.linkedin.common.FabricType.PROD);

    EnvelopedAspect keyAspect = new EnvelopedAspect();
    keyAspect.setValue(new Aspect(key.data()));

    Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(DATA_OBJECT_KEY_ASPECT_NAME, keyAspect);

    entityResponse.setAspects(new EnvelopedAspectMap(aspects));
    return entityResponse;
  }

  private void addAspectToResponse(
      EntityResponse entityResponse, String aspectName, Object aspectData) {
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new Aspect(((com.linkedin.data.template.RecordTemplate) aspectData).data()));
    entityResponse.getAspects().put(aspectName, aspect);
  }
}
