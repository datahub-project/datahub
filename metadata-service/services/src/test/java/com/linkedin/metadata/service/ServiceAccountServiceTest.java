package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.service.ServiceAccountService.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Unit tests for ServiceAccountService */
public class ServiceAccountServiceTest {

  private static final String TEST_SERVICE_ACCOUNT_NAME = "test-service";
  private static final String TEST_DISPLAY_NAME = "Test Service Account";
  private static final String TEST_DESCRIPTION = "A test service account";
  private static final String TEST_CREATED_BY = "urn:li:corpuser:admin";

  private SystemEntityClient mockEntityClient;
  private ServiceAccountService serviceAccountService;
  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    mockEntityClient = Mockito.mock(SystemEntityClient.class);
    serviceAccountService = new ServiceAccountService(mockEntityClient);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Test
  public void testCreateServiceAccountSuccess() throws Exception {
    // Arrange
    Urn expectedUrn = UrnUtils.getUrn("urn:li:corpuser:service:test-service");

    // Mock that service account doesn't exist
    when(mockEntityClient.exists(eq(opContext), eq(expectedUrn))).thenReturn(false);

    // Act
    Urn resultUrn =
        serviceAccountService.createServiceAccount(
            opContext,
            TEST_SERVICE_ACCOUNT_NAME,
            TEST_DISPLAY_NAME,
            TEST_DESCRIPTION,
            TEST_CREATED_BY);

    // Assert
    assertNotNull(resultUrn);
    assertEquals(resultUrn.toString(), expectedUrn.toString());

    // Verify entity client interactions
    verify(mockEntityClient).exists(eq(opContext), eq(expectedUrn));

    // Verify 3 proposals were ingested (CorpUserKey, CorpUserInfo, SubTypes)
    ArgumentCaptor<MetadataChangeProposal> mcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockEntityClient, times(3)).ingestProposal(eq(opContext), mcpCaptor.capture());

    // Verify CorpUserKey proposal
    MetadataChangeProposal keyProposal =
        mcpCaptor.getAllValues().stream()
            .filter(mcp -> CORP_USER_KEY_ASPECT_NAME.equals(mcp.getAspectName()))
            .findFirst()
            .orElse(null);
    assertNotNull(keyProposal);
    assertEquals(keyProposal.getEntityUrn(), expectedUrn);
    assertEquals(keyProposal.getEntityType(), CORP_USER_ENTITY_NAME);

    CorpUserKey key =
        GenericRecordUtils.deserializeAspect(
            keyProposal.getAspect().getValue(),
            keyProposal.getAspect().getContentType(),
            CorpUserKey.class);
    assertEquals(key.getUsername(), "service:test-service");

    // Verify CorpUserInfo proposal
    MetadataChangeProposal infoProposal =
        mcpCaptor.getAllValues().stream()
            .filter(mcp -> CORP_USER_INFO_ASPECT_NAME.equals(mcp.getAspectName()))
            .findFirst()
            .orElse(null);
    assertNotNull(infoProposal);
    assertEquals(infoProposal.getEntityUrn(), expectedUrn);

    CorpUserInfo info =
        GenericRecordUtils.deserializeAspect(
            infoProposal.getAspect().getValue(),
            infoProposal.getAspect().getContentType(),
            CorpUserInfo.class);
    assertTrue(info.isActive());
    assertEquals(info.getDisplayName(), TEST_DISPLAY_NAME);
    assertEquals(info.getTitle(), TEST_DESCRIPTION);

    // Verify SubTypes proposal
    MetadataChangeProposal subTypesProposal =
        mcpCaptor.getAllValues().stream()
            .filter(mcp -> SUB_TYPES_ASPECT_NAME.equals(mcp.getAspectName()))
            .findFirst()
            .orElse(null);
    assertNotNull(subTypesProposal);
    assertEquals(subTypesProposal.getEntityUrn(), expectedUrn);

    SubTypes subTypes =
        GenericRecordUtils.deserializeAspect(
            subTypesProposal.getAspect().getValue(),
            subTypesProposal.getAspect().getContentType(),
            SubTypes.class);
    assertTrue(subTypes.hasTypeNames());
    assertTrue(subTypes.getTypeNames().contains(SERVICE_ACCOUNT_SUB_TYPE));
  }

  @Test
  public void testCreateServiceAccountWithoutDescription() throws Exception {
    // Arrange
    Urn expectedUrn = UrnUtils.getUrn("urn:li:corpuser:service:test-service");
    when(mockEntityClient.exists(eq(opContext), eq(expectedUrn))).thenReturn(false);

    // Act
    Urn resultUrn =
        serviceAccountService.createServiceAccount(
            opContext, TEST_SERVICE_ACCOUNT_NAME, TEST_DISPLAY_NAME, null, TEST_CREATED_BY);

    // Assert
    assertNotNull(resultUrn);
    assertEquals(resultUrn.toString(), expectedUrn.toString());

    // Verify CorpUserInfo doesn't have title when description is null
    ArgumentCaptor<MetadataChangeProposal> mcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockEntityClient, times(3)).ingestProposal(eq(opContext), mcpCaptor.capture());

    MetadataChangeProposal infoProposal =
        mcpCaptor.getAllValues().stream()
            .filter(mcp -> CORP_USER_INFO_ASPECT_NAME.equals(mcp.getAspectName()))
            .findFirst()
            .orElse(null);
    assertNotNull(infoProposal);

    CorpUserInfo info =
        GenericRecordUtils.deserializeAspect(
            infoProposal.getAspect().getValue(),
            infoProposal.getAspect().getContentType(),
            CorpUserInfo.class);
    assertEquals(info.getDisplayName(), TEST_DISPLAY_NAME);
    assertFalse(info.hasTitle());
  }

  @Test
  public void testCreateServiceAccountWithoutDisplayName() throws Exception {
    // Arrange
    Urn expectedUrn = UrnUtils.getUrn("urn:li:corpuser:service:test-service");
    when(mockEntityClient.exists(eq(opContext), eq(expectedUrn))).thenReturn(false);

    // Act - displayName is null, should default to name
    Urn resultUrn =
        serviceAccountService.createServiceAccount(
            opContext, TEST_SERVICE_ACCOUNT_NAME, null, TEST_DESCRIPTION, TEST_CREATED_BY);

    // Assert
    assertNotNull(resultUrn);

    // Verify CorpUserInfo uses name as displayName when displayName is null
    ArgumentCaptor<MetadataChangeProposal> mcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockEntityClient, times(3)).ingestProposal(eq(opContext), mcpCaptor.capture());

    MetadataChangeProposal infoProposal =
        mcpCaptor.getAllValues().stream()
            .filter(mcp -> CORP_USER_INFO_ASPECT_NAME.equals(mcp.getAspectName()))
            .findFirst()
            .orElse(null);
    assertNotNull(infoProposal);

    CorpUserInfo info =
        GenericRecordUtils.deserializeAspect(
            infoProposal.getAspect().getValue(),
            infoProposal.getAspect().getContentType(),
            CorpUserInfo.class);
    assertEquals(info.getDisplayName(), TEST_SERVICE_ACCOUNT_NAME);
  }

  @Test(
      expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*already exists.*")
  public void testCreateServiceAccountAlreadyExists() throws Exception {
    // Arrange
    Urn expectedUrn = UrnUtils.getUrn("urn:li:corpuser:service:test-service");
    when(mockEntityClient.exists(eq(opContext), eq(expectedUrn))).thenReturn(true);

    // Act & Assert - should throw IllegalArgumentException
    serviceAccountService.createServiceAccount(
        opContext, TEST_SERVICE_ACCOUNT_NAME, TEST_DISPLAY_NAME, TEST_DESCRIPTION, TEST_CREATED_BY);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testCreateServiceAccountNullOpContext() throws Exception {
    // Act & Assert - should throw NullPointerException
    serviceAccountService.createServiceAccount(
        null, TEST_SERVICE_ACCOUNT_NAME, TEST_DISPLAY_NAME, TEST_DESCRIPTION, TEST_CREATED_BY);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testCreateServiceAccountNullName() throws Exception {
    // Act & Assert - should throw NullPointerException
    serviceAccountService.createServiceAccount(
        opContext, null, TEST_DISPLAY_NAME, TEST_DESCRIPTION, TEST_CREATED_BY);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testCreateServiceAccountNullCreatedBy() throws Exception {
    // Act & Assert - should throw NullPointerException
    serviceAccountService.createServiceAccount(
        opContext, TEST_SERVICE_ACCOUNT_NAME, TEST_DISPLAY_NAME, TEST_DESCRIPTION, null);
  }

  @Test
  public void testGetServiceAccountSuccess() throws Exception {
    // Arrange
    Urn serviceAccountUrn = UrnUtils.getUrn("urn:li:corpuser:service:test-service");

    // Create mock SubTypes aspect
    SubTypes subTypes = new SubTypes();
    subTypes.setTypeNames(new StringArray(SERVICE_ACCOUNT_SUB_TYPE));

    EntityResponse mockResponse = createMockEntityResponse(serviceAccountUrn, subTypes);

    // Mock first call to verify SubTypes, then second call to get all aspects
    when(mockEntityClient.getV2(
            eq(opContext),
            eq(CORP_USER_ENTITY_NAME),
            eq(serviceAccountUrn),
            eq(Collections.singleton(SUB_TYPES_ASPECT_NAME))))
        .thenReturn(mockResponse);
    when(mockEntityClient.getV2(
            eq(opContext), eq(CORP_USER_ENTITY_NAME), eq(serviceAccountUrn), eq(null)))
        .thenReturn(mockResponse);

    // Act
    EntityResponse result = serviceAccountService.getServiceAccount(opContext, serviceAccountUrn);

    // Assert
    assertNotNull(result);
    assertEquals(result.getUrn(), serviceAccountUrn);

    // Verify client was called twice - once to check SubTypes, once to get all aspects
    verify(mockEntityClient)
        .getV2(
            eq(opContext),
            eq(CORP_USER_ENTITY_NAME),
            eq(serviceAccountUrn),
            eq(Collections.singleton(SUB_TYPES_ASPECT_NAME)));
    verify(mockEntityClient)
        .getV2(eq(opContext), eq(CORP_USER_ENTITY_NAME), eq(serviceAccountUrn), eq(null));
  }

  @Test
  public void testGetServiceAccountNotFound() throws Exception {
    // Arrange
    Urn serviceAccountUrn = UrnUtils.getUrn("urn:li:corpuser:service:nonexistent");

    // Mock entity not found
    when(mockEntityClient.getV2(
            eq(opContext),
            eq(CORP_USER_ENTITY_NAME),
            eq(serviceAccountUrn),
            eq(Collections.singleton(SUB_TYPES_ASPECT_NAME))))
        .thenReturn(null);

    // Act
    EntityResponse result = serviceAccountService.getServiceAccount(opContext, serviceAccountUrn);

    // Assert
    assertNull(result);
    verify(mockEntityClient)
        .getV2(
            eq(opContext),
            eq(CORP_USER_ENTITY_NAME),
            eq(serviceAccountUrn),
            eq(Collections.singleton(SUB_TYPES_ASPECT_NAME)));
    // Should not call getV2 with null aspects
    verify(mockEntityClient, never())
        .getV2(eq(opContext), eq(CORP_USER_ENTITY_NAME), eq(serviceAccountUrn), eq(null));
  }

  @Test(
      expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*not a service account.*")
  public void testGetServiceAccountNotServiceAccount() throws Exception {
    // Arrange
    Urn regularUserUrn = UrnUtils.getUrn("urn:li:corpuser:regularuser");

    // Create mock response without SERVICE_ACCOUNT SubType
    SubTypes subTypes = new SubTypes();
    subTypes.setTypeNames(new StringArray("REGULAR_USER"));

    EntityResponse mockResponse = createMockEntityResponse(regularUserUrn, subTypes);

    when(mockEntityClient.getV2(
            eq(opContext),
            eq(CORP_USER_ENTITY_NAME),
            eq(regularUserUrn),
            eq(Collections.singleton(SUB_TYPES_ASPECT_NAME))))
        .thenReturn(mockResponse);

    // Act & Assert - should throw IllegalArgumentException
    serviceAccountService.getServiceAccount(opContext, regularUserUrn);
  }

  @Test(
      expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*not a service account.*")
  public void testGetServiceAccountNoSubTypes() throws Exception {
    // Arrange
    Urn regularUserUrn = UrnUtils.getUrn("urn:li:corpuser:regularuser");

    // Create mock response without SubTypes aspect
    EntityResponse mockResponse = new EntityResponse();
    mockResponse.setUrn(regularUserUrn);
    mockResponse.setAspects(new EnvelopedAspectMap());

    when(mockEntityClient.getV2(
            eq(opContext),
            eq(CORP_USER_ENTITY_NAME),
            eq(regularUserUrn),
            eq(Collections.singleton(SUB_TYPES_ASPECT_NAME))))
        .thenReturn(mockResponse);

    // Act & Assert - should throw IllegalArgumentException
    serviceAccountService.getServiceAccount(opContext, regularUserUrn);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testGetServiceAccountNullOpContext() throws Exception {
    // Arrange
    Urn serviceAccountUrn = UrnUtils.getUrn("urn:li:corpuser:service:test-service");

    // Act & Assert - should throw NullPointerException
    serviceAccountService.getServiceAccount(null, serviceAccountUrn);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testGetServiceAccountNullUrn() throws Exception {
    // Act & Assert - should throw NullPointerException
    serviceAccountService.getServiceAccount(opContext, null);
  }

  @Test
  public void testDeleteServiceAccountSuccess() throws Exception {
    // Arrange
    Urn serviceAccountUrn = UrnUtils.getUrn("urn:li:corpuser:service:test-service");

    // Create mock SubTypes aspect
    SubTypes subTypes = new SubTypes();
    subTypes.setTypeNames(new StringArray(SERVICE_ACCOUNT_SUB_TYPE));

    EntityResponse mockResponse = createMockEntityResponse(serviceAccountUrn, subTypes);

    when(mockEntityClient.getV2(
            eq(opContext),
            eq(CORP_USER_ENTITY_NAME),
            eq(serviceAccountUrn),
            eq(Collections.singleton(SUB_TYPES_ASPECT_NAME))))
        .thenReturn(mockResponse);

    // Act
    serviceAccountService.deleteServiceAccount(opContext, serviceAccountUrn);

    // Assert
    verify(mockEntityClient)
        .getV2(
            eq(opContext),
            eq(CORP_USER_ENTITY_NAME),
            eq(serviceAccountUrn),
            eq(Collections.singleton(SUB_TYPES_ASPECT_NAME)));
    verify(mockEntityClient).deleteEntity(eq(opContext), eq(serviceAccountUrn));
  }

  @Test(
      expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*not found.*")
  public void testDeleteServiceAccountNotFound() throws Exception {
    // Arrange
    Urn serviceAccountUrn = UrnUtils.getUrn("urn:li:corpuser:service:nonexistent");

    when(mockEntityClient.getV2(
            eq(opContext),
            eq(CORP_USER_ENTITY_NAME),
            eq(serviceAccountUrn),
            eq(Collections.singleton(SUB_TYPES_ASPECT_NAME))))
        .thenReturn(null);

    // Act & Assert - should throw IllegalArgumentException
    serviceAccountService.deleteServiceAccount(opContext, serviceAccountUrn);
  }

  @Test(
      expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*not a service account.*")
  public void testDeleteServiceAccountNotServiceAccount() throws Exception {
    // Arrange
    Urn regularUserUrn = UrnUtils.getUrn("urn:li:corpuser:regularuser");

    // Create mock response without SERVICE_ACCOUNT SubType
    SubTypes subTypes = new SubTypes();
    subTypes.setTypeNames(new StringArray("REGULAR_USER"));

    EntityResponse mockResponse = createMockEntityResponse(regularUserUrn, subTypes);

    when(mockEntityClient.getV2(
            eq(opContext),
            eq(CORP_USER_ENTITY_NAME),
            eq(regularUserUrn),
            eq(Collections.singleton(SUB_TYPES_ASPECT_NAME))))
        .thenReturn(mockResponse);

    // Act & Assert - should throw IllegalArgumentException
    serviceAccountService.deleteServiceAccount(opContext, regularUserUrn);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testDeleteServiceAccountNullOpContext() throws Exception {
    // Arrange
    Urn serviceAccountUrn = UrnUtils.getUrn("urn:li:corpuser:service:test-service");

    // Act & Assert - should throw NullPointerException
    serviceAccountService.deleteServiceAccount(null, serviceAccountUrn);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testDeleteServiceAccountNullUrn() throws Exception {
    // Act & Assert - should throw NullPointerException
    serviceAccountService.deleteServiceAccount(opContext, null);
  }

  @Test
  public void testIsServiceAccountTrue() {
    // Arrange
    Urn serviceAccountUrn = UrnUtils.getUrn("urn:li:corpuser:service:test-service");

    SubTypes subTypes = new SubTypes();
    subTypes.setTypeNames(new StringArray(SERVICE_ACCOUNT_SUB_TYPE));

    EntityResponse mockResponse = createMockEntityResponse(serviceAccountUrn, subTypes);

    // Act
    boolean result = ServiceAccountService.isServiceAccount(mockResponse);

    // Assert
    assertTrue(result);
  }

  @Test
  public void testIsServiceAccountFalseNoSubTypes() {
    // Arrange
    Urn regularUserUrn = UrnUtils.getUrn("urn:li:corpuser:regularuser");

    EntityResponse mockResponse = new EntityResponse();
    mockResponse.setUrn(regularUserUrn);
    mockResponse.setAspects(new EnvelopedAspectMap());

    // Act
    boolean result = ServiceAccountService.isServiceAccount(mockResponse);

    // Assert
    assertFalse(result);
  }

  @Test
  public void testIsServiceAccountFalseWrongSubType() {
    // Arrange
    Urn regularUserUrn = UrnUtils.getUrn("urn:li:corpuser:regularuser");

    SubTypes subTypes = new SubTypes();
    subTypes.setTypeNames(new StringArray("REGULAR_USER"));

    EntityResponse mockResponse = createMockEntityResponse(regularUserUrn, subTypes);

    // Act
    boolean result = ServiceAccountService.isServiceAccount(mockResponse);

    // Assert
    assertFalse(result);
  }

  @Test
  public void testIsServiceAccountNull() {
    // Act
    boolean result = ServiceAccountService.isServiceAccount(null);

    // Assert
    assertFalse(result);
  }

  @Test
  public void testIsServiceAccountWithMultipleSubTypes() {
    // Arrange
    Urn serviceAccountUrn = UrnUtils.getUrn("urn:li:corpuser:service:test-service");

    SubTypes subTypes = new SubTypes();
    StringArray typeNames = new StringArray();
    typeNames.add("OTHER_TYPE");
    typeNames.add(SERVICE_ACCOUNT_SUB_TYPE);
    typeNames.add("ANOTHER_TYPE");
    subTypes.setTypeNames(typeNames);

    EntityResponse mockResponse = createMockEntityResponse(serviceAccountUrn, subTypes);

    // Act
    boolean result = ServiceAccountService.isServiceAccount(mockResponse);

    // Assert
    assertTrue(result);
  }

  @Test
  public void testServiceAccountPrefixCorrect() {
    // Verify the prefix is correct
    assertEquals(SERVICE_ACCOUNT_PREFIX, "service:");
  }

  @Test
  public void testServiceAccountSubTypeCorrect() {
    // Verify the sub type constant is correct
    assertEquals(SERVICE_ACCOUNT_SUB_TYPE, "SERVICE_ACCOUNT");
  }

  // Helper method to create mock EntityResponse with SubTypes
  private EntityResponse createMockEntityResponse(Urn urn, SubTypes subTypes) {
    EntityResponse response = new EntityResponse();
    response.setUrn(urn);

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(subTypes.data()));
    aspectMap.put(SUB_TYPES_ASPECT_NAME, envelopedAspect);

    response.setAspects(aspectMap);

    return response;
  }
}
