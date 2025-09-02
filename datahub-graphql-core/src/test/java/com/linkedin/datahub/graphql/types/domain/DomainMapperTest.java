package com.linkedin.datahub.graphql.types.domain;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.DisplayProperties;
import com.linkedin.common.Forms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.DomainKey;
import com.linkedin.structured.StructuredProperties;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.mockito.MockedStatic;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DomainMapperTest {

  private static final String TEST_DOMAIN_URN = "urn:li:domain:marketing";
  private static final String TEST_DOMAIN_ID = "marketing";
  private static final String TEST_DOMAIN_NAME = "Marketing Domain";
  private static final String TEST_DOMAIN_DESCRIPTION = "Domain for marketing datasets";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:testuser";
  private static final Long TEST_TIMESTAMP = 1640995200000L; // 2022-01-01 00:00:00 UTC

  private Urn domainUrn;
  private Urn actorUrn;
  private QueryContext mockQueryContext;

  @BeforeMethod
  public void setup() throws URISyntaxException {
    domainUrn = Urn.createFromString(TEST_DOMAIN_URN);
    actorUrn = Urn.createFromString(TEST_ACTOR_URN);
    mockQueryContext = mock(QueryContext.class);
  }

  @Test
  public void testMapDomainWithAllAspects() throws URISyntaxException {
    // Setup entity response with all aspects
    EntityResponse entityResponse = createBasicEntityResponse();

    // Add domain properties
    DomainProperties domainProperties = new DomainProperties();
    domainProperties.setName(TEST_DOMAIN_NAME);
    domainProperties.setDescription(TEST_DOMAIN_DESCRIPTION);

    AuditStamp createdAuditStamp = new AuditStamp();
    createdAuditStamp.setTime(TEST_TIMESTAMP);
    createdAuditStamp.setActor(actorUrn);
    domainProperties.setCreated(createdAuditStamp);

    addAspectToResponse(entityResponse, DOMAIN_PROPERTIES_ASPECT_NAME, domainProperties);

    // Add ownership
    Ownership ownership = new Ownership();
    ownership.setOwners(new com.linkedin.common.OwnerArray()); // Empty but required field
    addAspectToResponse(entityResponse, OWNERSHIP_ASPECT_NAME, ownership);

    // Add institutional memory
    InstitutionalMemory institutionalMemory = new InstitutionalMemory();
    institutionalMemory.setElements(
        new com.linkedin.common.InstitutionalMemoryMetadataArray()); // Empty but required field
    addAspectToResponse(entityResponse, INSTITUTIONAL_MEMORY_ASPECT_NAME, institutionalMemory);

    // Add structured properties
    StructuredProperties structuredProperties = new StructuredProperties();
    structuredProperties.setProperties(
        new com.linkedin.structured
            .StructuredPropertyValueAssignmentArray()); // Empty but required field
    addAspectToResponse(entityResponse, STRUCTURED_PROPERTIES_ASPECT_NAME, structuredProperties);

    // Add forms
    Forms forms = new Forms();
    forms.setIncompleteForms(
        new com.linkedin.common.FormAssociationArray()); // Empty but required field
    forms.setCompletedForms(
        new com.linkedin.common.FormAssociationArray()); // Empty but required field
    addAspectToResponse(entityResponse, FORMS_ASPECT_NAME, forms);

    // Add display properties
    DisplayProperties displayProperties = new DisplayProperties();
    addAspectToResponse(entityResponse, DISPLAY_PROPERTIES_ASPECT_NAME, displayProperties);

    // Mock authorization
    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), eq(domainUrn))).thenReturn(true);

      // Execute mapping
      Domain result = DomainMapper.map(mockQueryContext, entityResponse);

      // Verify results
      assertNotNull(result);
      assertEquals(result.getUrn(), TEST_DOMAIN_URN);
      assertEquals(result.getType(), EntityType.DOMAIN);
      assertEquals(result.getId(), TEST_DOMAIN_ID);

      // Verify domain properties
      assertNotNull(result.getProperties());
      assertEquals(result.getProperties().getName(), TEST_DOMAIN_NAME);
      assertEquals(result.getProperties().getDescription(), TEST_DOMAIN_DESCRIPTION);

      // Verify created audit stamp from properties
      assertNotNull(result.getProperties().getCreatedOn());
      assertEquals(result.getProperties().getCreatedOn().getTime(), TEST_TIMESTAMP);
      assertEquals(result.getProperties().getCreatedOn().getActor().getUrn(), TEST_ACTOR_URN);

      // Verify other aspects are set
      assertNotNull(result.getOwnership());
      assertNotNull(result.getInstitutionalMemory());
      assertNotNull(result.getStructuredProperties());
      assertNotNull(result.getForms());
      assertNotNull(result.getDisplayProperties());
    }
  }

  @Test
  public void testMapDomainWithOnlyKeyAspect() {
    // Setup entity response with only domain key
    EntityResponse entityResponse = createBasicEntityResponse();

    // Mock authorization
    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), eq(domainUrn))).thenReturn(true);

      // Execute mapping
      Domain result = DomainMapper.map(mockQueryContext, entityResponse);

      // Verify results
      assertNotNull(result);
      assertEquals(result.getUrn(), TEST_DOMAIN_URN);
      assertEquals(result.getType(), EntityType.DOMAIN);
      assertEquals(result.getId(), TEST_DOMAIN_ID);

      // Verify optional aspects are null
      assertNull(result.getProperties());
      assertNull(result.getOwnership());
      assertNull(result.getInstitutionalMemory());
      assertNull(result.getStructuredProperties());
      assertNull(result.getForms());
      assertNull(result.getDisplayProperties());
    }
  }

  @Test
  public void testMapDomainWithCreatedAuditStampFallback() throws URISyntaxException {
    // Setup entity response with domain key that has created audit stamp
    EntityResponse entityResponse = createBasicEntityResponse();

    // Add domain properties without created timestamp (to trigger fallback)
    DomainProperties domainProperties = new DomainProperties();
    domainProperties.setName(TEST_DOMAIN_NAME);
    domainProperties.setDescription(TEST_DOMAIN_DESCRIPTION);
    // Note: NOT setting created timestamp to test fallback

    addAspectToResponse(entityResponse, DOMAIN_PROPERTIES_ASPECT_NAME, domainProperties);

    // Add created audit stamp to the domain key aspect (this is what gets extracted)
    EnvelopedAspect domainKeyAspect = entityResponse.getAspects().get(DOMAIN_KEY_ASPECT_NAME);
    AuditStamp keyCreatedStamp = new AuditStamp();
    keyCreatedStamp.setTime(TEST_TIMESTAMP);
    keyCreatedStamp.setActor(actorUrn);
    domainKeyAspect.setCreated(keyCreatedStamp);

    // Mock authorization
    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), eq(domainUrn))).thenReturn(true);

      // Execute mapping
      Domain result = DomainMapper.map(mockQueryContext, entityResponse);

      // Verify results
      assertNotNull(result);
      assertNotNull(result.getProperties());
      assertEquals(result.getProperties().getName(), TEST_DOMAIN_NAME);
      assertEquals(result.getProperties().getDescription(), TEST_DOMAIN_DESCRIPTION);

      // Verify fallback created audit stamp is used (from key aspect)
      assertNotNull(result.getProperties().getCreatedOn());
      assertEquals(result.getProperties().getCreatedOn().getTime(), TEST_TIMESTAMP);
      assertEquals(result.getProperties().getCreatedOn().getActor().getUrn(), TEST_ACTOR_URN);
    }
  }

  @Test
  public void testMapDomainWithPropertiesCreatedTimestampTakesPrecedence()
      throws URISyntaxException {
    // Setup entity response
    EntityResponse entityResponse = createBasicEntityResponse();

    // Add domain properties WITH created timestamp
    DomainProperties domainProperties = new DomainProperties();
    domainProperties.setName(TEST_DOMAIN_NAME);
    domainProperties.setDescription(TEST_DOMAIN_DESCRIPTION);

    Long propertiesTimestamp = TEST_TIMESTAMP + 1000L;
    AuditStamp createdAuditStamp = new AuditStamp();
    createdAuditStamp.setTime(propertiesTimestamp);
    createdAuditStamp.setActor(actorUrn);
    domainProperties.setCreated(createdAuditStamp);

    addAspectToResponse(entityResponse, DOMAIN_PROPERTIES_ASPECT_NAME, domainProperties);

    // Add created audit stamp to the domain key aspect (should be ignored)
    EnvelopedAspect domainKeyAspect = entityResponse.getAspects().get(DOMAIN_KEY_ASPECT_NAME);
    AuditStamp keyCreatedStamp = new AuditStamp();
    keyCreatedStamp.setTime(TEST_TIMESTAMP); // Different timestamp
    keyCreatedStamp.setActor(actorUrn);
    domainKeyAspect.setCreated(keyCreatedStamp);

    // Mock authorization
    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), eq(domainUrn))).thenReturn(true);

      // Execute mapping
      Domain result = DomainMapper.map(mockQueryContext, entityResponse);

      // Verify that properties created timestamp takes precedence (not the fallback)
      assertNotNull(result.getProperties().getCreatedOn());
      assertEquals(result.getProperties().getCreatedOn().getTime(), propertiesTimestamp);
      assertEquals(result.getProperties().getCreatedOn().getActor().getUrn(), TEST_ACTOR_URN);
    }
  }

  @Test
  public void testMapDomainWithCreatedTimestampWithoutActor() throws URISyntaxException {
    // Setup entity response
    EntityResponse entityResponse = createBasicEntityResponse();

    // Add domain properties with created timestamp but no actor
    DomainProperties domainProperties = new DomainProperties();
    domainProperties.setName(TEST_DOMAIN_NAME);

    AuditStamp createdAuditStamp = new AuditStamp();
    createdAuditStamp.setTime(TEST_TIMESTAMP);
    // Note: NOT setting actor to test GetMode.NULL handling
    domainProperties.setCreated(createdAuditStamp);

    addAspectToResponse(entityResponse, DOMAIN_PROPERTIES_ASPECT_NAME, domainProperties);

    // Mock authorization
    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), eq(domainUrn))).thenReturn(true);

      // Execute mapping
      Domain result = DomainMapper.map(mockQueryContext, entityResponse);

      // Verify created audit stamp is created with time but no actor
      assertNotNull(result.getProperties().getCreatedOn());
      assertEquals(result.getProperties().getCreatedOn().getTime(), TEST_TIMESTAMP);
      assertNull(result.getProperties().getCreatedOn().getActor());
    }
  }

  @Test
  public void testMapDomainWithoutDomainKey() {
    // Setup entity response without domain key aspect
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(domainUrn);
    entityResponse.setAspects(new EnvelopedAspectMap());

    // Execute mapping
    Domain result = DomainMapper.map(mockQueryContext, entityResponse);

    // Should return null when domain key aspect is missing
    assertNull(result);
  }

  @Test
  public void testMapDomainWithMissingDomainKey() {
    // Setup entity response with no domain key aspect (empty aspects map)
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(domainUrn);
    entityResponse.setAspects(new EnvelopedAspectMap()); // Empty map instead of null value

    // Execute mapping
    Domain result = DomainMapper.map(mockQueryContext, entityResponse);

    // Should return null when domain key aspect is missing
    assertNull(result);
  }

  @Test
  public void testMapDomainWithRestrictedAccess() {
    // Setup entity response
    EntityResponse entityResponse = createBasicEntityResponse();

    // Mock authorization to deny access
    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), eq(domainUrn))).thenReturn(false);

      Domain restrictedDomain = new Domain();
      authUtilsMock
          .when(() -> AuthorizationUtils.restrictEntity(any(Domain.class), eq(Domain.class)))
          .thenReturn(restrictedDomain);

      // Execute mapping
      Domain result = DomainMapper.map(mockQueryContext, entityResponse);

      // Should return restricted entity
      assertEquals(result, restrictedDomain);

      // Verify authorization calls
      authUtilsMock.verify(() -> AuthorizationUtils.canView(any(), eq(domainUrn)));
      authUtilsMock.verify(
          () -> AuthorizationUtils.restrictEntity(any(Domain.class), eq(Domain.class)));
    }
  }

  @Test
  public void testMapDomainWithNullQueryContext() {
    // Setup entity response
    EntityResponse entityResponse = createBasicEntityResponse();

    // Execute mapping with null query context
    Domain result = DomainMapper.map(null, entityResponse);

    // Should return domain without authorization checks
    assertNotNull(result);
    assertEquals(result.getUrn(), TEST_DOMAIN_URN);
    assertEquals(result.getType(), EntityType.DOMAIN);
    assertEquals(result.getId(), TEST_DOMAIN_ID);
  }

  @Test
  public void testMapDomainWithNoCreatedAuditStampFromKeyAspect() {
    // Setup entity response without created audit stamp in key aspect
    EntityResponse entityResponse = createBasicEntityResponse();

    // Test without created audit stamp by not setting one initially in the basic response
    // Note: createBasicEntityResponse() doesn't set created by default

    // Add domain properties without created timestamp
    DomainProperties domainProperties = new DomainProperties();
    domainProperties.setName(TEST_DOMAIN_NAME);
    addAspectToResponse(entityResponse, DOMAIN_PROPERTIES_ASPECT_NAME, domainProperties);

    // Mock authorization
    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), eq(domainUrn))).thenReturn(true);

      // Execute mapping
      Domain result = DomainMapper.map(mockQueryContext, entityResponse);

      // Should handle null fallback gracefully
      assertNotNull(result);
      assertNotNull(result.getProperties());
      assertEquals(result.getProperties().getName(), TEST_DOMAIN_NAME);
      assertNull(result.getProperties().getCreatedOn()); // Should be null when fallback is null
    }
  }

  @Test
  public void testMapDomainWithEmptyDomainProperties() {
    // Setup entity response
    EntityResponse entityResponse = createBasicEntityResponse();

    // Add empty domain properties
    DomainProperties domainProperties = new DomainProperties();
    domainProperties.setName("Test Domain"); // Required field
    addAspectToResponse(entityResponse, DOMAIN_PROPERTIES_ASPECT_NAME, domainProperties);

    // Mock authorization
    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), eq(domainUrn))).thenReturn(true);

      // Execute mapping
      Domain result = DomainMapper.map(mockQueryContext, entityResponse);

      // Should handle empty properties
      assertNotNull(result);
      assertNotNull(result.getProperties());
      assertEquals(
          result.getProperties().getName(), "Test Domain"); // We set a name because it's required
      assertNull(result.getProperties().getDescription());
      assertNull(result.getProperties().getCreatedOn());
    }
  }

  // Helper methods

  private EntityResponse createBasicEntityResponse() {
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(domainUrn);

    // Create domain key aspect
    DomainKey domainKey = new DomainKey();
    domainKey.setId(TEST_DOMAIN_ID);

    EnvelopedAspect domainKeyAspect = new EnvelopedAspect();
    domainKeyAspect.setValue(new Aspect(domainKey.data()));

    Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(DOMAIN_KEY_ASPECT_NAME, domainKeyAspect);

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
