package com.linkedin.datahub.graphql.types.knowledge;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
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
  private static final Long TEST_TIMESTAMP = 1640995200000L; // 2022-01-01 00:00:00 UTC

  private Urn documentUrn;
  private Urn actorUrn;
  private Urn parentUrn;
  private Urn assetUrn;
  private Urn relatedDocumentUrn;
  private QueryContext mockQueryContext;

  @BeforeMethod
  public void setup() throws URISyntaxException {
    documentUrn = Urn.createFromString(TEST_DOCUMENT_URN);
    actorUrn = Urn.createFromString(TEST_ACTOR_URN);
    parentUrn = Urn.createFromString(TEST_PARENT_URN);
    assetUrn = Urn.createFromString(TEST_ASSET_URN);
    relatedDocumentUrn = Urn.createFromString(TEST_RELATED_DOCUMENT_URN);
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

    // Add institutional memory
    InstitutionalMemory institutionalMemory = new InstitutionalMemory();
    institutionalMemory.setElements(new com.linkedin.common.InstitutionalMemoryMetadataArray());
    addAspectToResponse(entityResponse, INSTITUTIONAL_MEMORY_ASPECT_NAME, institutionalMemory);

    // Add structured properties
    StructuredProperties structuredProperties = new StructuredProperties();
    structuredProperties.setProperties(
        new com.linkedin.structured.StructuredPropertyValueAssignmentArray());
    addAspectToResponse(entityResponse, STRUCTURED_PROPERTIES_ASPECT_NAME, structuredProperties);

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

      // Relationships are present inside info and constructed as unresolved stubs
      assertNotNull(result.getInfo().getParentDocument());
      assertNotNull(result.getInfo().getRelatedAssets());
      assertNotNull(result.getInfo().getRelatedDocuments());

      // Verify other aspects
      assertNotNull(result.getOwnership());
      assertNotNull(result.getInstitutionalMemory());
      assertNotNull(result.getStructuredProperties());
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
      assertNull(result.getInstitutionalMemory());
      assertNull(result.getStructuredProperties());
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
