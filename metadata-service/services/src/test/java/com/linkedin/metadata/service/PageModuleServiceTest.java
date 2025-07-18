package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.datahub.authorization.AuthUtil;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.module.DataHubPageModuleParams;
import com.linkedin.module.DataHubPageModuleProperties;
import com.linkedin.module.DataHubPageModuleType;
import com.linkedin.module.DataHubPageModuleVisibility;
import com.linkedin.module.PageModuleScope;
import com.linkedin.module.RichTextModuleParams;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.exception.UnauthorizedException;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PageModuleServiceTest {

  private static final String TEST_MODULE_URN = "urn:li:dataHubPageModule:test-module";
  private static final String TEST_MODULE_NAME = "Test Module";
  private static final String TEST_RICH_TEXT_CONTENT = "Test content";

  @Mock private EntityClient mockEntityClient;
  @Mock private OperationContext mockOpContext;

  private PageModuleService service;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    service = new PageModuleService(mockEntityClient);
  }

  @Test
  public void testUpsertPageModuleSuccessWithUrn() throws Exception {
    // Arrange
    Urn moduleUrn = UrnUtils.getUrn(TEST_MODULE_URN);
    DataHubPageModuleType type = DataHubPageModuleType.RICH_TEXT;
    PageModuleScope scope = PageModuleScope.PERSONAL;
    DataHubPageModuleParams params = createTestParams();

    when(mockOpContext.getAuditStamp()).thenReturn(createTestAuditStamp());
    when(mockEntityClient.batchIngestProposals(any(), any(), eq(false))).thenReturn(null);

    // Act
    Urn result =
        service.upsertPageModule(
            mockOpContext, TEST_MODULE_URN, TEST_MODULE_NAME, type, scope, params);

    // Assert
    assertEquals(result, moduleUrn);
    verify(mockEntityClient, times(1)).batchIngestProposals(any(), any(), eq(false));
  }

  @Test
  public void testUpsertPageModuleSuccessWithGeneratedUrn() throws Exception {
    // Arrange
    DataHubPageModuleType type = DataHubPageModuleType.RICH_TEXT;
    PageModuleScope scope = PageModuleScope.PERSONAL;
    DataHubPageModuleParams params = createTestParams();

    when(mockOpContext.getAuditStamp()).thenReturn(createTestAuditStamp());
    when(mockEntityClient.batchIngestProposals(any(), any(), eq(false))).thenReturn(null);

    // Act
    Urn result =
        service.upsertPageModule(mockOpContext, null, TEST_MODULE_NAME, type, scope, params);

    // Assert
    assertNotNull(result);
    assertEquals(result.getEntityType(), "dataHubPageModule");
    verify(mockEntityClient, times(1)).batchIngestProposals(any(), any(), eq(false));
  }

  @Test
  public void testUpsertPageModuleFailure() throws Exception {
    // Arrange
    DataHubPageModuleType type = DataHubPageModuleType.RICH_TEXT;
    PageModuleScope scope = PageModuleScope.PERSONAL;
    DataHubPageModuleParams params = createTestParams();

    when(mockOpContext.getAuditStamp()).thenReturn(createTestAuditStamp());
    when(mockEntityClient.batchIngestProposals(any(), any(), eq(false)))
        .thenThrow(new RuntimeException("Test exception"));

    // Act & Assert
    assertThrows(
        RuntimeException.class,
        () -> {
          service.upsertPageModule(
              mockOpContext, TEST_MODULE_URN, TEST_MODULE_NAME, type, scope, params);
        });
  }

  @Test
  public void testGetPageModulePropertiesSuccess() throws Exception {
    // Arrange
    Urn moduleUrn = UrnUtils.getUrn(TEST_MODULE_URN);
    DataHubPageModuleProperties expectedProperties = createTestModuleProperties();
    EntityResponse mockResponse = createMockEntityResponse(moduleUrn, expectedProperties);

    when(mockEntityClient.getV2(
            any(),
            eq(Constants.DATAHUB_PAGE_MODULE_ENTITY_NAME),
            eq(moduleUrn),
            eq(null),
            eq(false)))
        .thenReturn(mockResponse);

    // Act
    DataHubPageModuleProperties result = service.getPageModuleProperties(mockOpContext, moduleUrn);

    // Assert
    assertNotNull(result);
    assertEquals(result.getName(), TEST_MODULE_NAME);
    assertEquals(result.getType(), DataHubPageModuleType.RICH_TEXT);
  }

  @Test
  public void testGetPageModulePropertiesNotFound() throws Exception {
    // Arrange
    Urn moduleUrn = UrnUtils.getUrn(TEST_MODULE_URN);

    when(mockEntityClient.getV2(
            any(),
            eq(Constants.DATAHUB_PAGE_MODULE_ENTITY_NAME),
            eq(moduleUrn),
            eq(null),
            eq(false)))
        .thenReturn(null);

    // Act
    DataHubPageModuleProperties result = service.getPageModuleProperties(mockOpContext, moduleUrn);

    // Assert
    assertEquals(result, null);
  }

  @Test
  public void testGetPageModuleEntityResponseSuccess() throws Exception {
    // Arrange
    Urn moduleUrn = UrnUtils.getUrn(TEST_MODULE_URN);
    EntityResponse expectedResponse =
        createMockEntityResponse(moduleUrn, createTestModuleProperties());

    when(mockEntityClient.getV2(
            any(),
            eq(Constants.DATAHUB_PAGE_MODULE_ENTITY_NAME),
            eq(moduleUrn),
            eq(null),
            eq(false)))
        .thenReturn(expectedResponse);

    // Act
    EntityResponse result = service.getPageModuleEntityResponse(mockOpContext, moduleUrn);

    // Assert
    assertNotNull(result);
    assertEquals(result.getUrn(), moduleUrn);
  }

  @Test
  public void testGetPageModuleEntityResponseFailure() throws Exception {
    // Arrange
    Urn moduleUrn = UrnUtils.getUrn(TEST_MODULE_URN);

    when(mockEntityClient.getV2(
            any(),
            eq(Constants.DATAHUB_PAGE_MODULE_ENTITY_NAME),
            eq(moduleUrn),
            eq(null),
            eq(false)))
        .thenThrow(new RuntimeException("Test exception"));

    // Act & Assert
    assertThrows(
        RuntimeException.class,
        () -> {
          service.getPageModuleEntityResponse(mockOpContext, moduleUrn);
        });
  }

  @Test
  public void testDeletePageModuleSuccess() throws Exception {
    // Arrange
    Urn moduleUrn = UrnUtils.getUrn(TEST_MODULE_URN);
    // Mock getPageModuleProperties to return a non-null value so permission check passes
    PageModuleService spyService = org.mockito.Mockito.spy(service);
    org.mockito.Mockito.doReturn(createTestModuleProperties())
        .when(spyService)
        .getPageModuleProperties(mockOpContext, moduleUrn);

    // Mock actor context and actor URN
    io.datahubproject.metadata.context.ActorContext mockActorContext =
        org.mockito.Mockito.mock(io.datahubproject.metadata.context.ActorContext.class);
    org.mockito.Mockito.when(mockOpContext.getActorContext()).thenReturn(mockActorContext);
    org.mockito.Mockito.when(mockActorContext.getActorUrn())
        .thenReturn(UrnUtils.getUrn("urn:li:corpuser:test-user"));

    // Mock AuthUtil to return false for MANAGE_HOME_PAGE_TEMPLATES_PRIVILEGE (should not have
    // permission)
    try (MockedStatic<AuthUtil> authUtilMock = mockStatic(AuthUtil.class)) {
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAuthorized(
                      mockOpContext, PoliciesConfig.MANAGE_HOME_PAGE_TEMPLATES_PRIVILEGE))
          .thenReturn(false);

      // Act
      spyService.deletePageModule(mockOpContext, moduleUrn);

      // Assert
      verify(mockEntityClient, times(1)).deleteEntity(mockOpContext, moduleUrn);
    }
  }

  @Test
  public void testDeletePageModuleFailure() throws Exception {
    // Arrange
    Urn moduleUrn = UrnUtils.getUrn(TEST_MODULE_URN);

    doThrow(new RuntimeException("Test exception"))
        .when(mockEntityClient)
        .deleteEntity(any(), any());

    // Act & Assert
    assertThrows(
        RuntimeException.class,
        () -> {
          service.deletePageModule(mockOpContext, moduleUrn);
        });
  }

  @Test
  public void testDeletePageModuleWithNullUrn() throws Exception {
    // Act & Assert
    assertThrows(
        NullPointerException.class,
        () -> {
          service.deletePageModule(mockOpContext, null);
        });
  }

  @Test
  public void testDeletePageModuleDefaultModulePrevention() throws Exception {
    // Test that default modules cannot be deleted
    String[] defaultModuleUrns = {
      "urn:li:dataHubPageModule:your_assets",
      "urn:li:dataHubPageModule:your_subscriptions",
      "urn:li:dataHubPageModule:top_domains"
    };

    for (String defaultModuleUrn : defaultModuleUrns) {
      Urn moduleUrn = UrnUtils.getUrn(defaultModuleUrn);
      try {
        service.deletePageModule(mockOpContext, moduleUrn);
        fail("Should not be able to delete default module: " + defaultModuleUrn);
      } catch (RuntimeException ex) {
        assertTrue(ex.getCause() instanceof IllegalArgumentException);
        assertTrue(
            ex.getCause()
                .getMessage()
                .contains("Cannot delete default page module with urn " + defaultModuleUrn));
      }
    }
  }

  @Test
  public void testDeletePersonalPageModuleNotCreatedByActor() throws Exception {
    // Arrange
    Urn moduleUrn = UrnUtils.getUrn(TEST_MODULE_URN);
    PageModuleService spyService = org.mockito.Mockito.spy(service);
    // Create properties with PERSONAL scope and a different creator
    DataHubPageModuleProperties properties = createTestModuleProperties();
    properties.getVisibility().setScope(com.linkedin.module.PageModuleScope.PERSONAL);
    // Set creator to someone else
    com.linkedin.common.urn.Urn otherUserUrn = UrnUtils.getUrn("urn:li:corpuser:other-user");
    properties.getCreated().setActor(otherUserUrn);
    org.mockito.Mockito.doReturn(properties)
        .when(spyService)
        .getPageModuleProperties(mockOpContext, moduleUrn);

    // Mock actor context and actor URN (the actor is NOT the creator)
    io.datahubproject.metadata.context.ActorContext mockActorContext =
        org.mockito.Mockito.mock(io.datahubproject.metadata.context.ActorContext.class);
    org.mockito.Mockito.when(mockOpContext.getActorContext()).thenReturn(mockActorContext);
    org.mockito.Mockito.when(mockActorContext.getActorUrn())
        .thenReturn(UrnUtils.getUrn("urn:li:corpuser:test-user"));

    // Mock AuthUtil to return false for MANAGE_HOME_PAGE_TEMPLATES_PRIVILEGE (should not have
    // permission)
    try (MockedStatic<AuthUtil> authUtilMock = mockStatic(AuthUtil.class)) {
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAuthorized(
                      mockOpContext, PoliciesConfig.MANAGE_HOME_PAGE_TEMPLATES_PRIVILEGE))
          .thenReturn(false);

      // Act & Assert
      try {
        spyService.deletePageModule(mockOpContext, moduleUrn);
        fail("Should not be able to delete a PERSONAL page module not created by the actor");
      } catch (RuntimeException ex) {
        assertTrue(ex.getCause() instanceof UnauthorizedException);
        assertTrue(
            ex.getCause()
                .getMessage()
                .contains(
                    "Attempted to delete personal a page module that was not created by the actor"));
      }
    }
  }

  @Test
  public void testDeleteGlobalPageModuleWithManagePermissionThrowsUnauthorized() throws Exception {
    // Arrange
    Urn moduleUrn = UrnUtils.getUrn(TEST_MODULE_URN);
    PageModuleService spyService = org.mockito.Mockito.spy(service);
    // Create properties with GLOBAL scope
    DataHubPageModuleProperties properties = createTestModuleProperties();
    properties.getVisibility().setScope(com.linkedin.module.PageModuleScope.GLOBAL);
    org.mockito.Mockito.doReturn(properties)
        .when(spyService)
        .getPageModuleProperties(mockOpContext, moduleUrn);

    // Mock actor context and actor URN
    io.datahubproject.metadata.context.ActorContext mockActorContext =
        org.mockito.Mockito.mock(io.datahubproject.metadata.context.ActorContext.class);
    org.mockito.Mockito.when(mockOpContext.getActorContext()).thenReturn(mockActorContext);
    org.mockito.Mockito.when(mockActorContext.getActorUrn())
        .thenReturn(UrnUtils.getUrn("urn:li:corpuser:test-user"));

    // Mock AuthUtil to return true for MANAGE_HOME_PAGE_TEMPLATES_PRIVILEGE
    try (MockedStatic<AuthUtil> authUtilMock = mockStatic(AuthUtil.class)) {
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAuthorized(
                      mockOpContext, PoliciesConfig.MANAGE_HOME_PAGE_TEMPLATES_PRIVILEGE))
          .thenReturn(true);

      // Act & Assert
      try {
        spyService.deletePageModule(mockOpContext, moduleUrn);
        fail(
            "Should throw UnauthorizedException when user has manage privilege for personal module");
      } catch (RuntimeException ex) {
        assertTrue(ex.getCause() instanceof UnauthorizedException);
        assertTrue(
            ex.getCause().getMessage().contains("User is unauthorized to delete global modules"));
      }
    }
  }

  @Test
  public void testDeleteGlobalPageModuleWithoutManagePermissionSuccess() throws Exception {
    // Arrange
    Urn moduleUrn = UrnUtils.getUrn(TEST_MODULE_URN);
    PageModuleService spyService = org.mockito.Mockito.spy(service);
    // Create properties with GLOBAL scope
    DataHubPageModuleProperties properties = createTestModuleProperties();
    properties.getVisibility().setScope(com.linkedin.module.PageModuleScope.GLOBAL);
    org.mockito.Mockito.doReturn(properties)
        .when(spyService)
        .getPageModuleProperties(mockOpContext, moduleUrn);

    // Mock actor context and actor URN
    io.datahubproject.metadata.context.ActorContext mockActorContext =
        org.mockito.Mockito.mock(io.datahubproject.metadata.context.ActorContext.class);
    org.mockito.Mockito.when(mockOpContext.getActorContext()).thenReturn(mockActorContext);
    org.mockito.Mockito.when(mockActorContext.getActorUrn())
        .thenReturn(UrnUtils.getUrn("urn:li:corpuser:test-user"));

    // Mock AuthUtil to return false for MANAGE_HOME_PAGE_TEMPLATES_PRIVILEGE
    try (MockedStatic<AuthUtil> authUtilMock = mockStatic(AuthUtil.class)) {
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAuthorized(
                      mockOpContext, PoliciesConfig.MANAGE_HOME_PAGE_TEMPLATES_PRIVILEGE))
          .thenReturn(false);

      // Act
      spyService.deletePageModule(mockOpContext, moduleUrn);

      // Assert
      verify(mockEntityClient, times(1)).deleteEntity(mockOpContext, moduleUrn);
    }
  }

  private DataHubPageModuleParams createTestParams() {
    DataHubPageModuleParams params = new DataHubPageModuleParams();
    RichTextModuleParams richTextParams = new RichTextModuleParams();
    richTextParams.setContent(TEST_RICH_TEXT_CONTENT);
    params.setRichTextParams(richTextParams);
    return params;
  }

  private DataHubPageModuleProperties createTestModuleProperties() {
    DataHubPageModuleProperties properties = new DataHubPageModuleProperties();
    properties.setName(TEST_MODULE_NAME);
    properties.setType(DataHubPageModuleType.RICH_TEXT);

    DataHubPageModuleVisibility visibility = new DataHubPageModuleVisibility();
    visibility.setScope(PageModuleScope.PERSONAL);
    properties.setVisibility(visibility);

    properties.setParams(createTestParams());
    properties.setCreated(createTestAuditStamp());
    properties.setLastModified(createTestAuditStamp());

    return properties;
  }

  private AuditStamp createTestAuditStamp() {
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(UrnUtils.getUrn("urn:li:corpuser:test-user"));
    return auditStamp;
  }

  private EntityResponse createMockEntityResponse(
      Urn moduleUrn, DataHubPageModuleProperties properties) {
    EntityResponse response = new EntityResponse();
    response.setUrn(moduleUrn);

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new com.linkedin.entity.Aspect(properties.data()));
    aspectMap.put(Constants.DATAHUB_PAGE_MODULE_PROPERTIES_ASPECT_NAME, aspect);
    response.setAspects(aspectMap);

    return response;
  }
}
