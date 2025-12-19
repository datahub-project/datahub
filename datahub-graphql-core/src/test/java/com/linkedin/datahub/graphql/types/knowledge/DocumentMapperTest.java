package com.linkedin.datahub.graphql.types.knowledge;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.Document;
import com.linkedin.datahub.graphql.generated.DocumentSourceType;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.knowledge.DocumentContents;
import com.linkedin.knowledge.DocumentInfo;
import com.linkedin.knowledge.DocumentSource;
import com.linkedin.knowledge.ParentDocument;
import com.linkedin.knowledge.RelatedAsset;
import com.linkedin.knowledge.RelatedAssetArray;
import com.linkedin.knowledge.RelatedDocument;
import com.linkedin.knowledge.RelatedDocumentArray;
import com.linkedin.metadata.key.DocumentKey;
import com.linkedin.structured.StructuredProperties;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.mockito.MockedStatic;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DocumentMapperTest {

  private static final String TEST_DOCUMENT_URN = "urn:li:document:test-document";
  private static final String TEST_DOCUMENT_ID = "test-document";
  private static final String TEST_DOCUMENT_TYPE = "tutorial";
  private static final String TEST_DOCUMENT_TITLE = "Test Tutorial";
  private static final String TEST_CONTENT = "Test content";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:testuser";
  private static final String TEST_PARENT_URN = "urn:li:document:parent-document";
  private static final String TEST_ASSET_URN = "urn:li:dataset:test-dataset";
  private static final String TEST_RELATED_DOCUMENT_URN = "urn:li:document:related-document";
  private static final String TEST_PLATFORM_URN = "urn:li:dataPlatform:confluence";
  private static final String TEST_PLATFORM_INSTANCE_URN =
      "urn:li:dataPlatformInstance:(urn:li:dataPlatform:confluence,prod)";
  private static final String DEFAULT_DATAHUB_PLATFORM_URN = "urn:li:dataPlatform:datahub";
  private static final Long TEST_TIMESTAMP = 1640995200000L; // 2022-01-01 00:00:00 UTC

  private Urn documentUrn;
  private Urn actorUrn;
  private Urn parentUrn;
  private Urn assetUrn;
  private Urn relatedDocumentUrn;
  private Urn platformUrn;
  private Urn platformInstanceUrn;
  private QueryContext mockQueryContext;

  @BeforeMethod
  public void setup() throws URISyntaxException {
    documentUrn = Urn.createFromString(TEST_DOCUMENT_URN);
    actorUrn = Urn.createFromString(TEST_ACTOR_URN);
    parentUrn = Urn.createFromString(TEST_PARENT_URN);
    assetUrn = Urn.createFromString(TEST_ASSET_URN);
    relatedDocumentUrn = Urn.createFromString(TEST_RELATED_DOCUMENT_URN);
    platformUrn = Urn.createFromString(TEST_PLATFORM_URN);
    platformInstanceUrn = Urn.createFromString(TEST_PLATFORM_INSTANCE_URN);
    mockQueryContext = mock(QueryContext.class);
  }

  @Test
  public void testMapDocumentWithAllAspects() throws URISyntaxException {
    // Setup entity response with all aspects
    EntityResponse entityResponse = createBasicEntityResponse();

    // Add document info
    DocumentInfo documentInfo = new DocumentInfo();
    documentInfo.setTitle(TEST_DOCUMENT_TITLE);

    DocumentContents contents = new DocumentContents();
    contents.setText(TEST_CONTENT);
    documentInfo.setContents(contents);

    AuditStamp createdStamp = new AuditStamp();
    createdStamp.setTime(TEST_TIMESTAMP);
    createdStamp.setActor(actorUrn);
    documentInfo.setCreated(createdStamp);

    AuditStamp lastModifiedStamp = new AuditStamp();
    lastModifiedStamp.setTime(TEST_TIMESTAMP);
    lastModifiedStamp.setActor(actorUrn);
    documentInfo.setLastModified(lastModifiedStamp);

    // Add custom properties
    com.linkedin.data.template.StringMap customProperties =
        new com.linkedin.data.template.StringMap();
    customProperties.put("key1", "value1");
    customProperties.put("key2", "value2");
    documentInfo.setCustomProperties(customProperties);

    addAspectToResponse(entityResponse, DOCUMENT_INFO_ASPECT_NAME, documentInfo);

    // Embed relationships inside DocumentInfo
    ParentDocument parentDocument = new ParentDocument();
    parentDocument.setDocument(parentUrn);
    documentInfo.setParentDocument(parentDocument);

    RelatedAsset relatedAsset = new RelatedAsset();
    relatedAsset.setAsset(assetUrn);
    RelatedAssetArray assetsArray = new RelatedAssetArray();
    assetsArray.add(relatedAsset);
    documentInfo.setRelatedAssets(assetsArray);

    RelatedDocument relatedDocument = new RelatedDocument();
    relatedDocument.setDocument(relatedDocumentUrn);
    RelatedDocumentArray documentsArray = new RelatedDocumentArray();
    documentsArray.add(relatedDocument);
    documentInfo.setRelatedDocuments(documentsArray);

    // Add ownership
    Ownership ownership = new Ownership();
    ownership.setOwners(new com.linkedin.common.OwnerArray());
    addAspectToResponse(entityResponse, OWNERSHIP_ASPECT_NAME, ownership);

    // Add structured properties
    StructuredProperties structuredProperties = new StructuredProperties();
    structuredProperties.setProperties(
        new com.linkedin.structured.StructuredPropertyValueAssignmentArray());
    addAspectToResponse(entityResponse, STRUCTURED_PROPERTIES_ASPECT_NAME, structuredProperties);

    // Add global tags
    GlobalTags globalTags = new GlobalTags();
    globalTags.setTags(new com.linkedin.common.TagAssociationArray());
    addAspectToResponse(entityResponse, GLOBAL_TAGS_ASPECT_NAME, globalTags);

    // Add glossary terms
    GlossaryTerms glossaryTerms = new GlossaryTerms();
    glossaryTerms.setTerms(new com.linkedin.common.GlossaryTermAssociationArray());
    glossaryTerms.setAuditStamp(new AuditStamp().setTime(TEST_TIMESTAMP).setActor(actorUrn));
    addAspectToResponse(entityResponse, GLOSSARY_TERMS_ASPECT_NAME, glossaryTerms);

    // Mock authorization
    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), eq(documentUrn))).thenReturn(true);

      // Execute mapping
      Document result = DocumentMapper.map(mockQueryContext, entityResponse);

      // Verify results
      assertNotNull(result);
      assertEquals(result.getUrn(), TEST_DOCUMENT_URN);
      assertEquals(result.getType(), EntityType.DOCUMENT);

      // Verify document info
      assertNotNull(result.getInfo());
      assertEquals(result.getInfo().getTitle(), TEST_DOCUMENT_TITLE);
      assertEquals(result.getInfo().getContents().getText(), TEST_CONTENT);
      assertNotNull(result.getInfo().getCreated());
      assertEquals(result.getInfo().getCreated().getTime(), TEST_TIMESTAMP);
      // Verify actor is set as CorpUser in ResolvedAuditStamp
      assertNotNull(result.getInfo().getCreated().getActor());
      assertEquals(result.getInfo().getCreated().getActor().getUrn(), TEST_ACTOR_URN);
      assertNotNull(result.getInfo().getLastModified());
      assertEquals(result.getInfo().getLastModified().getTime(), TEST_TIMESTAMP);
      assertNotNull(result.getInfo().getLastModified().getActor());
      assertEquals(result.getInfo().getLastModified().getActor().getUrn(), TEST_ACTOR_URN);

      // Relationships are present inside info and constructed as unresolved stubs
      assertNotNull(result.getInfo().getParentDocument());
      assertNotNull(result.getInfo().getRelatedAssets());
      assertNotNull(result.getInfo().getRelatedDocuments());

      // Verify other aspects
      assertNotNull(result.getOwnership());
      assertNotNull(result.getStructuredProperties());
      assertNotNull(result.getTags());
      assertNotNull(result.getGlossaryTerms());

      // Verify custom properties
      assertNotNull(result.getInfo().getCustomProperties());
      assertEquals(result.getInfo().getCustomProperties().size(), 2);
      assertEquals(result.getInfo().getCustomProperties().get(0).getKey(), "key1");
      assertEquals(result.getInfo().getCustomProperties().get(0).getValue(), "value1");
    }
  }

  @Test
  public void testMapDocumentWithOnlyKeyAndInfo() throws URISyntaxException {
    // Setup entity response with only key and info
    EntityResponse entityResponse = createBasicEntityResponse();

    // Add minimal document info
    DocumentInfo documentInfo = new DocumentInfo();

    DocumentContents contents = new DocumentContents();
    contents.setText(TEST_CONTENT);
    documentInfo.setContents(contents);

    AuditStamp createdStamp = new AuditStamp();
    createdStamp.setTime(TEST_TIMESTAMP);
    createdStamp.setActor(actorUrn);
    documentInfo.setCreated(createdStamp);

    AuditStamp lastModifiedStamp = new AuditStamp();
    lastModifiedStamp.setTime(TEST_TIMESTAMP);
    lastModifiedStamp.setActor(actorUrn);
    documentInfo.setLastModified(lastModifiedStamp);

    addAspectToResponse(entityResponse, DOCUMENT_INFO_ASPECT_NAME, documentInfo);

    // Mock authorization
    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), eq(documentUrn))).thenReturn(true);

      // Execute mapping
      Document result = DocumentMapper.map(mockQueryContext, entityResponse);

      // Verify results
      assertNotNull(result);
      assertEquals(result.getUrn(), TEST_DOCUMENT_URN);
      assertEquals(result.getType(), EntityType.DOCUMENT);

      // Verify document info
      assertNotNull(result.getInfo());
      assertNull(result.getInfo().getTitle()); // title was not set

      // Verify optional relationships are null when not provided
      assertNull(result.getInfo().getParentDocument());
      assertNull(result.getInfo().getRelatedAssets());
      assertNull(result.getInfo().getRelatedDocuments());
      assertNull(result.getOwnership());
      assertNull(result.getStructuredProperties());
      assertNull(result.getTags());
      assertNull(result.getGlossaryTerms());
    }
  }

  @Test
  public void testMapDocumentWithoutKey() {
    // Setup entity response without key aspect
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(documentUrn);
    entityResponse.setAspects(new EnvelopedAspectMap());

    // Mock authorization
    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), eq(documentUrn))).thenReturn(true);

      // Execute mapping
      Document result = DocumentMapper.map(mockQueryContext, entityResponse);

      // Should return entity with just urn and type when key aspect is missing
      assertNotNull(result);
      assertEquals(result.getUrn(), TEST_DOCUMENT_URN);
      assertEquals(result.getType(), EntityType.DOCUMENT);
    }
  }

  @Test
  public void testMapDocumentWithRestrictedAccess() {
    // Setup entity response
    EntityResponse entityResponse = createBasicEntityResponse();

    // Mock authorization to deny access
    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock
          .when(() -> AuthorizationUtils.canView(any(), eq(documentUrn)))
          .thenReturn(false);

      Document restrictedDocument = new Document();
      authUtilsMock
          .when(() -> AuthorizationUtils.restrictEntity(any(Document.class), eq(Document.class)))
          .thenReturn(restrictedDocument);

      // Execute mapping
      Document result = DocumentMapper.map(mockQueryContext, entityResponse);

      // Should return restricted entity
      assertEquals(result, restrictedDocument);

      // Verify authorization calls
      authUtilsMock.verify(() -> AuthorizationUtils.canView(any(), eq(documentUrn)));
      authUtilsMock.verify(
          () -> AuthorizationUtils.restrictEntity(any(Document.class), eq(Document.class)));
    }
  }

  @Test
  public void testMapDocumentWithNullQueryContext() {
    // Setup entity response
    EntityResponse entityResponse = createBasicEntityResponse();

    // Execute mapping with null query context
    Document result = DocumentMapper.map(null, entityResponse);

    // Should return document without authorization checks
    assertNotNull(result);
    assertEquals(result.getUrn(), TEST_DOCUMENT_URN);
    assertEquals(result.getType(), EntityType.DOCUMENT);
  }

  @Test
  public void testMapDocumentSourceNative() throws URISyntaxException {
    // Setup entity response with NATIVE source
    EntityResponse entityResponse = createBasicEntityResponse();

    // Add document info with NATIVE source
    DocumentInfo documentInfo = new DocumentInfo();

    DocumentContents contents = new DocumentContents();
    contents.setText(TEST_CONTENT);
    documentInfo.setContents(contents);

    AuditStamp createdStamp = new AuditStamp();
    createdStamp.setTime(TEST_TIMESTAMP);
    createdStamp.setActor(actorUrn);
    documentInfo.setCreated(createdStamp);

    AuditStamp lastModifiedStamp = new AuditStamp();
    lastModifiedStamp.setTime(TEST_TIMESTAMP);
    lastModifiedStamp.setActor(actorUrn);
    documentInfo.setLastModified(lastModifiedStamp);

    // Add NATIVE source
    DocumentSource source = new DocumentSource();
    source.setSourceType(com.linkedin.knowledge.DocumentSourceType.NATIVE);
    documentInfo.setSource(source);

    addAspectToResponse(entityResponse, DOCUMENT_INFO_ASPECT_NAME, documentInfo);

    // Mock authorization
    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), eq(documentUrn))).thenReturn(true);

      // Execute mapping
      Document result = DocumentMapper.map(mockQueryContext, entityResponse);

      // Verify source is mapped correctly
      assertNotNull(result.getInfo().getSource());
      assertEquals(result.getInfo().getSource().getSourceType(), DocumentSourceType.NATIVE);
    }
  }

  @Test
  public void testMapDocumentSourceExternal() throws URISyntaxException {
    // Setup entity response with EXTERNAL source
    EntityResponse entityResponse = createBasicEntityResponse();

    // Add document info with EXTERNAL source
    DocumentInfo documentInfo = new DocumentInfo();

    DocumentContents contents = new DocumentContents();
    contents.setText(TEST_CONTENT);
    documentInfo.setContents(contents);

    AuditStamp createdStamp = new AuditStamp();
    createdStamp.setTime(TEST_TIMESTAMP);
    createdStamp.setActor(actorUrn);
    documentInfo.setCreated(createdStamp);

    AuditStamp lastModifiedStamp = new AuditStamp();
    lastModifiedStamp.setTime(TEST_TIMESTAMP);
    lastModifiedStamp.setActor(actorUrn);
    documentInfo.setLastModified(lastModifiedStamp);

    // Add EXTERNAL source with additional fields
    DocumentSource source = new DocumentSource();
    source.setSourceType(com.linkedin.knowledge.DocumentSourceType.EXTERNAL);
    source.setExternalUrl("https://external.com/doc");
    source.setExternalId("ext-123");
    documentInfo.setSource(source);

    addAspectToResponse(entityResponse, DOCUMENT_INFO_ASPECT_NAME, documentInfo);

    // Mock authorization
    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), eq(documentUrn))).thenReturn(true);

      // Execute mapping
      Document result = DocumentMapper.map(mockQueryContext, entityResponse);

      // Verify source is mapped correctly
      assertNotNull(result.getInfo().getSource());
      assertEquals(result.getInfo().getSource().getSourceType(), DocumentSourceType.EXTERNAL);
      assertEquals(result.getInfo().getSource().getExternalUrl(), "https://external.com/doc");
      assertEquals(result.getInfo().getSource().getExternalId(), "ext-123");
    }
  }

  @Test
  public void testMapDocumentWithSubTypes() throws URISyntaxException {
    // Setup entity response with SubTypes
    EntityResponse entityResponse = createBasicEntityResponse();

    // Add minimal document info
    DocumentInfo documentInfo = new DocumentInfo();
    DocumentContents contents = new DocumentContents();
    contents.setText(TEST_CONTENT);
    documentInfo.setContents(contents);
    AuditStamp createdStamp = new AuditStamp();
    createdStamp.setTime(TEST_TIMESTAMP);
    createdStamp.setActor(actorUrn);
    documentInfo.setCreated(createdStamp);
    documentInfo.setLastModified(createdStamp);
    addAspectToResponse(entityResponse, DOCUMENT_INFO_ASPECT_NAME, documentInfo);

    // Add SubTypes aspect
    com.linkedin.common.SubTypes subTypes = new com.linkedin.common.SubTypes();
    subTypes.setTypeNames(
        new com.linkedin.data.template.StringArray(java.util.Arrays.asList("tutorial", "guide")));
    addAspectToResponse(entityResponse, SUB_TYPES_ASPECT_NAME, subTypes);

    // Mock authorization
    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), eq(documentUrn))).thenReturn(true);

      // Execute mapping
      Document result = DocumentMapper.map(mockQueryContext, entityResponse);

      // Verify subType is set to the first type
      assertNotNull(result.getSubType());
      assertEquals(result.getSubType(), "tutorial");
    }
  }

  @Test
  public void testMapDocumentWithDomains() throws URISyntaxException {
    // Setup entity response with Domains
    EntityResponse entityResponse = createBasicEntityResponse();

    // Add minimal document info
    DocumentInfo documentInfo = new DocumentInfo();
    DocumentContents contents = new DocumentContents();
    contents.setText(TEST_CONTENT);
    documentInfo.setContents(contents);
    AuditStamp createdStamp = new AuditStamp();
    createdStamp.setTime(TEST_TIMESTAMP);
    createdStamp.setActor(actorUrn);
    documentInfo.setCreated(createdStamp);
    documentInfo.setLastModified(createdStamp);
    addAspectToResponse(entityResponse, DOCUMENT_INFO_ASPECT_NAME, documentInfo);

    // Add Domains aspect
    com.linkedin.domain.Domains domains = new com.linkedin.domain.Domains();
    Urn domainUrn = Urn.createFromString("urn:li:domain:test-domain");
    com.linkedin.common.UrnArray domainUrns = new com.linkedin.common.UrnArray();
    domainUrns.add(domainUrn);
    domains.setDomains(domainUrns);
    addAspectToResponse(entityResponse, DOMAINS_ASPECT_NAME, domains);

    // Mock authorization
    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), eq(documentUrn))).thenReturn(true);

      // Execute mapping - should not throw exception
      Document result = DocumentMapper.map(mockQueryContext, entityResponse);

      // Verify result is not null and has basic fields
      assertNotNull(result);
      assertEquals(result.getUrn(), TEST_DOCUMENT_URN);
      // Domain mapping is handled if query context and domains are set up properly
    }
  }

  @Test
  public void testMapDocumentWithStatusRemoved() throws URISyntaxException {
    // Setup entity response with Status aspect indicating soft delete
    EntityResponse entityResponse = createBasicEntityResponse();

    // Add minimal document info
    DocumentInfo documentInfo = new DocumentInfo();
    DocumentContents contents = new DocumentContents();
    contents.setText(TEST_CONTENT);
    documentInfo.setContents(contents);
    AuditStamp createdStamp = new AuditStamp();
    createdStamp.setTime(TEST_TIMESTAMP);
    createdStamp.setActor(actorUrn);
    documentInfo.setCreated(createdStamp);
    documentInfo.setLastModified(createdStamp);
    addAspectToResponse(entityResponse, DOCUMENT_INFO_ASPECT_NAME, documentInfo);

    // Add Status aspect with removed = true
    com.linkedin.common.Status status = new com.linkedin.common.Status();
    status.setRemoved(true);
    addAspectToResponse(entityResponse, STATUS_ASPECT_NAME, status);

    // Mock authorization
    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), eq(documentUrn))).thenReturn(true);

      // Execute mapping
      Document result = DocumentMapper.map(mockQueryContext, entityResponse);

      // Verify exists is set to false when removed
      assertNotNull(result.getExists());
      assertFalse(result.getExists());
    }
  }

  @Test
  public void testMapDocumentWithStatusNotRemoved() throws URISyntaxException {
    // Setup entity response with Status aspect indicating not removed
    EntityResponse entityResponse = createBasicEntityResponse();

    // Add minimal document info
    DocumentInfo documentInfo = new DocumentInfo();
    DocumentContents contents = new DocumentContents();
    contents.setText(TEST_CONTENT);
    documentInfo.setContents(contents);
    AuditStamp createdStamp = new AuditStamp();
    createdStamp.setTime(TEST_TIMESTAMP);
    createdStamp.setActor(actorUrn);
    documentInfo.setCreated(createdStamp);
    documentInfo.setLastModified(createdStamp);
    addAspectToResponse(entityResponse, DOCUMENT_INFO_ASPECT_NAME, documentInfo);

    // Add Status aspect with removed = false
    com.linkedin.common.Status status = new com.linkedin.common.Status();
    status.setRemoved(false);
    addAspectToResponse(entityResponse, STATUS_ASPECT_NAME, status);

    // Mock authorization
    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), eq(documentUrn))).thenReturn(true);

      // Execute mapping
      Document result = DocumentMapper.map(mockQueryContext, entityResponse);

      // Verify exists is set to true when not removed
      assertNotNull(result.getExists());
      assertTrue(result.getExists());
    }
  }

  @Test
  public void testMapDocumentWithDocumentState() throws URISyntaxException {
    // Setup entity response with DocumentState
    EntityResponse entityResponse = createBasicEntityResponse();

    // Add document info with state
    DocumentInfo documentInfo = new DocumentInfo();
    DocumentContents contents = new DocumentContents();
    contents.setText(TEST_CONTENT);
    documentInfo.setContents(contents);

    AuditStamp createdStamp = new AuditStamp();
    createdStamp.setTime(TEST_TIMESTAMP);
    createdStamp.setActor(actorUrn);
    documentInfo.setCreated(createdStamp);
    documentInfo.setLastModified(createdStamp);

    // Add status with state
    com.linkedin.knowledge.DocumentStatus status = new com.linkedin.knowledge.DocumentStatus();
    status.setState(com.linkedin.knowledge.DocumentState.PUBLISHED);
    documentInfo.setStatus(status);

    addAspectToResponse(entityResponse, DOCUMENT_INFO_ASPECT_NAME, documentInfo);

    // Mock authorization
    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), eq(documentUrn))).thenReturn(true);

      // Execute mapping
      Document result = DocumentMapper.map(mockQueryContext, entityResponse);

      // Verify status is mapped
      assertNotNull(result.getInfo().getStatus());
      assertEquals(
          result.getInfo().getStatus().getState(),
          com.linkedin.datahub.graphql.generated.DocumentState.PUBLISHED);
    }
  }

  @Test
  public void testMapDocumentWithDefaultPlatform() throws URISyntaxException {
    // Setup entity response WITHOUT DataPlatformInstance aspect
    EntityResponse entityResponse = createBasicEntityResponse();

    // Add minimal document info
    DocumentInfo documentInfo = new DocumentInfo();
    DocumentContents contents = new DocumentContents();
    contents.setText(TEST_CONTENT);
    documentInfo.setContents(contents);
    AuditStamp createdStamp = new AuditStamp();
    createdStamp.setTime(TEST_TIMESTAMP);
    createdStamp.setActor(actorUrn);
    documentInfo.setCreated(createdStamp);
    documentInfo.setLastModified(createdStamp);
    addAspectToResponse(entityResponse, DOCUMENT_INFO_ASPECT_NAME, documentInfo);

    // Mock authorization
    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), eq(documentUrn))).thenReturn(true);

      // Execute mapping
      Document result = DocumentMapper.map(mockQueryContext, entityResponse);

      // Verify platform defaults to datahub
      assertNotNull(result.getPlatform());
      assertEquals(result.getPlatform().getUrn(), DEFAULT_DATAHUB_PLATFORM_URN);
      assertEquals(result.getPlatform().getType(), EntityType.DATA_PLATFORM);

      // Verify dataPlatformInstance is not set
      assertNull(result.getDataPlatformInstance());
    }
  }

  @Test
  public void testMapDocumentWithDataPlatformInstanceWithoutInstance() throws URISyntaxException {
    // Setup entity response with DataPlatformInstance aspect (platform only, no instance)
    EntityResponse entityResponse = createBasicEntityResponse();

    // Add minimal document info
    DocumentInfo documentInfo = new DocumentInfo();
    DocumentContents contents = new DocumentContents();
    contents.setText(TEST_CONTENT);
    documentInfo.setContents(contents);
    AuditStamp createdStamp = new AuditStamp();
    createdStamp.setTime(TEST_TIMESTAMP);
    createdStamp.setActor(actorUrn);
    documentInfo.setCreated(createdStamp);
    documentInfo.setLastModified(createdStamp);
    addAspectToResponse(entityResponse, DOCUMENT_INFO_ASPECT_NAME, documentInfo);

    // Add DataPlatformInstance aspect with platform only
    com.linkedin.common.DataPlatformInstance dataPlatformInstance =
        new com.linkedin.common.DataPlatformInstance();
    dataPlatformInstance.setPlatform(new DataPlatformUrn("confluence"));
    addAspectToResponse(entityResponse, DATA_PLATFORM_INSTANCE_ASPECT_NAME, dataPlatformInstance);

    // Mock authorization
    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), eq(documentUrn))).thenReturn(true);

      // Execute mapping
      Document result = DocumentMapper.map(mockQueryContext, entityResponse);

      // Verify platform is set from DataPlatformInstance
      assertNotNull(result.getPlatform());
      assertEquals(result.getPlatform().getUrn(), TEST_PLATFORM_URN);
      assertEquals(result.getPlatform().getType(), EntityType.DATA_PLATFORM);

      // Verify dataPlatformInstance is NOT set when there's no instance
      assertNull(result.getDataPlatformInstance());
    }
  }

  @Test
  public void testMapDocumentWithDataPlatformInstanceWithInstance() throws URISyntaxException {
    // Setup entity response with DataPlatformInstance aspect (with both platform and instance)
    EntityResponse entityResponse = createBasicEntityResponse();

    // Add minimal document info
    DocumentInfo documentInfo = new DocumentInfo();
    DocumentContents contents = new DocumentContents();
    contents.setText(TEST_CONTENT);
    documentInfo.setContents(contents);
    AuditStamp createdStamp = new AuditStamp();
    createdStamp.setTime(TEST_TIMESTAMP);
    createdStamp.setActor(actorUrn);
    documentInfo.setCreated(createdStamp);
    documentInfo.setLastModified(createdStamp);
    addAspectToResponse(entityResponse, DOCUMENT_INFO_ASPECT_NAME, documentInfo);

    // Add DataPlatformInstance aspect with both platform and instance
    com.linkedin.common.DataPlatformInstance dataPlatformInstance =
        new com.linkedin.common.DataPlatformInstance();
    dataPlatformInstance.setPlatform(new DataPlatformUrn("confluence"));
    dataPlatformInstance.setInstance(platformInstanceUrn);
    addAspectToResponse(entityResponse, DATA_PLATFORM_INSTANCE_ASPECT_NAME, dataPlatformInstance);

    // Mock authorization
    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), eq(documentUrn))).thenReturn(true);

      // Execute mapping
      Document result = DocumentMapper.map(mockQueryContext, entityResponse);

      // Verify platform is set from DataPlatformInstance
      assertNotNull(result.getPlatform());
      assertEquals(result.getPlatform().getUrn(), TEST_PLATFORM_URN);
      assertEquals(result.getPlatform().getType(), EntityType.DATA_PLATFORM);

      // Verify dataPlatformInstance IS set when there's an instance
      assertNotNull(result.getDataPlatformInstance());
      assertEquals(result.getDataPlatformInstance().getUrn(), TEST_PLATFORM_INSTANCE_URN);
      assertEquals(result.getDataPlatformInstance().getType(), EntityType.DATA_PLATFORM_INSTANCE);
    }
  }

  // Helper methods

  private EntityResponse createBasicEntityResponse() {
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(documentUrn);

    // Create document key aspect
    DocumentKey documentKey = new DocumentKey();
    documentKey.setId(TEST_DOCUMENT_ID);

    EnvelopedAspect keyAspect = new EnvelopedAspect();
    keyAspect.setValue(new Aspect(documentKey.data()));

    Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(DOCUMENT_KEY_ASPECT_NAME, keyAspect);

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
