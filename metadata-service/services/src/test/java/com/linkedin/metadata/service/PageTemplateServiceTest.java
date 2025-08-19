package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthUtil;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.key.DataHubPageTemplateKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.template.DataHubPageTemplateProperties;
import com.linkedin.template.DataHubPageTemplateRow;
import com.linkedin.template.DataHubPageTemplateSurface;
import com.linkedin.template.DataHubPageTemplateVisibility;
import com.linkedin.template.PageTemplateScope;
import com.linkedin.template.PageTemplateSurfaceType;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.exception.UnauthorizedException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.mockito.MockedStatic;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PageTemplateServiceTest {
  private EntityClient mockEntityClient;
  private PageTemplateService service;
  private OperationContext mockOpContext;
  private Urn templateUrn;
  private DataHubPageTemplateKey key;

  @BeforeMethod
  public void setup() throws Exception {
    mockEntityClient = mock(EntityClient.class);
    service = new PageTemplateService(mockEntityClient);
    mockOpContext = mock(OperationContext.class);
    key = new DataHubPageTemplateKey().setId("test-id");
    templateUrn =
        EntityKeyUtils.convertEntityKeyToUrn(key, Constants.DATAHUB_PAGE_TEMPLATE_ENTITY_NAME);
    when(mockOpContext.getAuditStamp()).thenReturn(new AuditStamp());
  }

  @Test
  public void testUpsertPageTemplateSuccessWithUrn() throws Exception {
    when(mockEntityClient.batchIngestProposals(any(), anyList(), eq(false))).thenReturn(null);
    when(mockEntityClient.exists(any(), any())).thenReturn(true);

    List<DataHubPageTemplateRow> rows = createTestRows();
    Urn urn =
        service.upsertPageTemplate(
            mockOpContext,
            templateUrn.toString(),
            rows,
            PageTemplateScope.GLOBAL,
            PageTemplateSurfaceType.HOME_PAGE);
    assertNotNull(urn);
    assertEquals(urn.toString(), templateUrn.toString());
    verify(mockEntityClient, times(1)).batchIngestProposals(any(), anyList(), eq(false));
    verify(mockEntityClient, times(2)).exists(any(), any()); // 2 modules in test rows
  }

  @Test
  public void testUpsertPageTemplateSuccessWithGeneratedUrn() throws Exception {
    when(mockEntityClient.batchIngestProposals(any(), anyList(), eq(false))).thenReturn(null);
    when(mockEntityClient.exists(any(), any())).thenReturn(true);

    List<DataHubPageTemplateRow> rows = createTestRows();
    Urn urn =
        service.upsertPageTemplate(
            mockOpContext,
            null, // null urn should generate a new one
            rows,
            PageTemplateScope.GLOBAL,
            PageTemplateSurfaceType.HOME_PAGE);
    assertNotNull(urn);
    assertTrue(urn.toString().startsWith("urn:li:dataHubPageTemplate:"));
    verify(mockEntityClient, times(1)).batchIngestProposals(any(), anyList(), eq(false));
    verify(mockEntityClient, times(2)).exists(any(), any()); // 2 modules in test rows
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testUpsertPageTemplateFailure() throws Exception {
    doThrow(new RuntimeException("fail"))
        .when(mockEntityClient)
        .batchIngestProposals(any(), anyList(), eq(false));
    when(mockEntityClient.exists(any(), any())).thenReturn(true);

    service.upsertPageTemplate(
        mockOpContext,
        templateUrn.toString(),
        Collections.emptyList(),
        PageTemplateScope.GLOBAL,
        PageTemplateSurfaceType.HOME_PAGE);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testUpsertPageTemplateWithNonExistentModule() throws Exception {
    when(mockEntityClient.exists(any(), any())).thenReturn(false);

    List<DataHubPageTemplateRow> rows = createTestRows();
    service.upsertPageTemplate(
        mockOpContext,
        templateUrn.toString(),
        rows,
        PageTemplateScope.GLOBAL,
        PageTemplateSurfaceType.HOME_PAGE);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testUpsertPageTemplateWithModuleValidationException() throws Exception {
    when(mockEntityClient.exists(any(), any()))
        .thenThrow(new RuntimeException("Validation failed"));

    List<DataHubPageTemplateRow> rows = createTestRows();
    service.upsertPageTemplate(
        mockOpContext,
        templateUrn.toString(),
        rows,
        PageTemplateScope.GLOBAL,
        PageTemplateSurfaceType.HOME_PAGE);
  }

  @Test
  public void testGetPageTemplatePropertiesFound() throws Exception {
    DataHubPageTemplateProperties properties = new DataHubPageTemplateProperties();
    EntityResponse response = mock(EntityResponse.class);
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new com.linkedin.entity.Aspect(properties.data()));
    aspectMap.put(Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME, aspect);
    when(response.getAspects()).thenReturn(aspectMap);
    when(mockEntityClient.getV2(any(), anyString(), any(), any(), eq(false))).thenReturn(response);
    DataHubPageTemplateProperties result =
        service.getPageTemplateProperties(mockOpContext, templateUrn);
    assertNotNull(result);
  }

  @Test
  public void testGetPageTemplatePropertiesNotFound() throws Exception {
    EntityResponse response = mock(EntityResponse.class);
    EnvelopedAspectMap emptyAspectMap = new EnvelopedAspectMap();
    when(response.getAspects()).thenReturn(emptyAspectMap);
    when(mockEntityClient.getV2(any(), anyString(), any(), any(), eq(false))).thenReturn(response);
    DataHubPageTemplateProperties result =
        service.getPageTemplateProperties(mockOpContext, templateUrn);
    assertNull(result);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetPageTemplateEntityResponseThrows() throws Exception {
    when(mockEntityClient.getV2(any(), anyString(), any(), any(), eq(false)))
        .thenThrow(new RuntimeException("fail"));
    service.getPageTemplateEntityResponse(mockOpContext, templateUrn);
  }

  @Test
  public void testDeletePageTemplateSuccess() throws Exception {
    // Arrange
    Urn templateUrn = UrnUtils.getUrn("urn:li:dataHubPageTemplate:test");
    // Mock getPageTemplateProperties to return a non-null value so permission check passes
    PageTemplateService spyService = org.mockito.Mockito.spy(service);
    org.mockito.Mockito.doReturn(createTestTemplateProperties())
        .when(spyService)
        .getPageTemplateProperties(mockOpContext, templateUrn);

    // Mock actor context and actor URN
    Authentication mockSessionAuth = org.mockito.Mockito.mock(Authentication.class);
    org.mockito.Mockito.when(mockOpContext.getSessionAuthentication()).thenReturn(mockSessionAuth);
    org.mockito.Mockito.when(mockSessionAuth.getActor())
        .thenReturn(new Actor(ActorType.USER, "test-user"));

    // Mock AuthUtil to return false for MANAGE_HOME_PAGE_TEMPLATES_PRIVILEGE
    try (MockedStatic<AuthUtil> authUtilMock = mockStatic(AuthUtil.class)) {
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAuthorized(
                      mockOpContext, PoliciesConfig.MANAGE_HOME_PAGE_TEMPLATES_PRIVILEGE))
          .thenReturn(false);

      // Act
      spyService.deletePageTemplate(mockOpContext, templateUrn);

      // Assert
      verify(mockEntityClient, times(1)).deleteEntity(mockOpContext, templateUrn);
    }
  }

  @Test
  public void testDeletePageTemplateFailure() throws Exception {
    // Arrange
    Urn templateUrn = UrnUtils.getUrn("urn:li:dataHubPageTemplate:test");

    doThrow(new RuntimeException("Test exception"))
        .when(mockEntityClient)
        .deleteEntity(any(), any());

    // Act & Assert
    assertThrows(
        RuntimeException.class,
        () -> {
          service.deletePageTemplate(mockOpContext, templateUrn);
        });
  }

  @Test
  public void testDeletePageTemplateWithNullUrn() throws Exception {
    // Act & Assert
    assertThrows(
        NullPointerException.class,
        () -> {
          service.deletePageTemplate(mockOpContext, null);
        });
  }

  @Test
  public void testDeletePageTemplateNonExistent() throws Exception {
    // Arrange
    Urn templateUrn = UrnUtils.getUrn("urn:li:dataHubPageTemplate:test");
    PageTemplateService spyService = org.mockito.Mockito.spy(service);
    // Mock getPageTemplateProperties to return null (template doesn't exist)
    org.mockito.Mockito.doReturn(null)
        .when(spyService)
        .getPageTemplateProperties(mockOpContext, templateUrn);

    // Act & Assert
    try {
      spyService.deletePageTemplate(mockOpContext, templateUrn);
      fail("Should not be able to delete non-existent template");
    } catch (RuntimeException ex) {
      assertTrue(ex.getCause() instanceof IllegalArgumentException);
      assertTrue(
          ex.getCause()
              .getMessage()
              .contains("Attempted to delete a page template that does not exist with urn"));
    }
  }

  @Test
  public void testDeletePersonalPageTemplateNotCreatedByActor() throws Exception {
    // Arrange
    Urn templateUrn = UrnUtils.getUrn("urn:li:dataHubPageTemplate:test");
    PageTemplateService spyService = org.mockito.Mockito.spy(service);
    // Create properties with PERSONAL scope and a different creator
    DataHubPageTemplateProperties properties = createTestTemplateProperties();
    properties.getVisibility().setScope(PageTemplateScope.PERSONAL);
    // Set creator to someone else
    com.linkedin.common.urn.Urn otherUserUrn = UrnUtils.getUrn("urn:li:corpuser:other-user");
    properties.getCreated().setActor(otherUserUrn);
    org.mockito.Mockito.doReturn(properties)
        .when(spyService)
        .getPageTemplateProperties(mockOpContext, templateUrn);

    // Mock actor context and actor URN (the actor is NOT the creator)
    Authentication mockSessionAuth = org.mockito.Mockito.mock(Authentication.class);
    org.mockito.Mockito.when(mockOpContext.getSessionAuthentication()).thenReturn(mockSessionAuth);
    org.mockito.Mockito.when(mockSessionAuth.getActor())
        .thenReturn(new Actor(ActorType.USER, "test-user"));

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
        spyService.deletePageTemplate(mockOpContext, templateUrn);
        fail("Should not be able to delete a PERSONAL page template not created by the actor");
      } catch (RuntimeException ex) {
        assertTrue(ex.getCause() instanceof UnauthorizedException);
        assertTrue(
            ex.getCause()
                .getMessage()
                .contains(
                    "Attempted to delete personal a page template that was not created by the actor"));
      }
    }
  }

  @Test
  public void testDeleteGlobalPageTemplateWithoutManagePermissionThrowsUnauthorized()
      throws Exception {
    // Arrange
    Urn templateUrn = UrnUtils.getUrn("urn:li:dataHubPageTemplate:test");
    PageTemplateService spyService = org.mockito.Mockito.spy(service);
    // Create properties with GLOBAL scope
    DataHubPageTemplateProperties properties = createTestTemplateProperties();
    properties.getVisibility().setScope(PageTemplateScope.GLOBAL);
    org.mockito.Mockito.doReturn(properties)
        .when(spyService)
        .getPageTemplateProperties(mockOpContext, templateUrn);

    // Mock AuthUtil to return false for MANAGE_HOME_PAGE_TEMPLATES_PRIVILEGE
    try (MockedStatic<AuthUtil> authUtilMock = mockStatic(AuthUtil.class)) {
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAuthorized(
                      mockOpContext, PoliciesConfig.MANAGE_HOME_PAGE_TEMPLATES_PRIVILEGE))
          .thenReturn(false);

      // Act & Assert
      try {
        spyService.deletePageTemplate(mockOpContext, templateUrn);
        fail(
            "Should throw UnauthorizedException when user doesn't have manage privilege for global template");
      } catch (RuntimeException ex) {
        assertTrue(ex.getCause() instanceof UnauthorizedException);
        assertTrue(
            ex.getCause().getMessage().contains("User is unauthorized to delete global templates"));
      }
    }
  }

  @Test
  public void testDeleteGlobalPageTemplateWithManagePermissionSuccess() throws Exception {
    // Arrange
    Urn templateUrn = UrnUtils.getUrn("urn:li:dataHubPageTemplate:test");
    PageTemplateService spyService = org.mockito.Mockito.spy(service);
    // Create properties with GLOBAL scope
    DataHubPageTemplateProperties properties = createTestTemplateProperties();
    properties.getVisibility().setScope(PageTemplateScope.GLOBAL);
    org.mockito.Mockito.doReturn(properties)
        .when(spyService)
        .getPageTemplateProperties(mockOpContext, templateUrn);

    // Mock AuthUtil to return true for MANAGE_HOME_PAGE_TEMPLATES_PRIVILEGE
    try (MockedStatic<AuthUtil> authUtilMock = mockStatic(AuthUtil.class)) {
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAuthorized(
                      mockOpContext, PoliciesConfig.MANAGE_HOME_PAGE_TEMPLATES_PRIVILEGE))
          .thenReturn(true);

      // Act
      spyService.deletePageTemplate(mockOpContext, templateUrn);

      // Assert
      verify(mockEntityClient, times(1)).deleteEntity(mockOpContext, templateUrn);
    }
  }

  @Test
  public void testDeleteDefaultPageTemplateWithManagePermission() throws Exception {
    // Arrange
    Urn templateUrn = UrnUtils.getUrn("urn:li:dataHubPageTemplate:home_default_1");
    PageTemplateService spyService = org.mockito.Mockito.spy(service);
    // Create properties with GLOBAL scope
    DataHubPageTemplateProperties properties = createTestTemplateProperties();
    properties.getVisibility().setScope(PageTemplateScope.GLOBAL);
    org.mockito.Mockito.doReturn(properties)
        .when(spyService)
        .getPageTemplateProperties(mockOpContext, templateUrn);

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
        spyService.deletePageTemplate(mockOpContext, templateUrn);
        fail(
            "Should throw UnauthorizedException when user doesn't have manage privilege for global template");
      } catch (RuntimeException ex) {
        assertTrue(ex.getCause() instanceof UnauthorizedException);
        assertTrue(
            ex.getCause().getMessage().contains("Attempted to delete the default page template"));
      }
    }
  }

  private DataHubPageTemplateProperties createTestTemplateProperties() {
    DataHubPageTemplateProperties properties = new DataHubPageTemplateProperties();

    // Set rows
    properties.setRows(new com.linkedin.template.DataHubPageTemplateRowArray(createTestRows()));

    // Set surface
    DataHubPageTemplateSurface surface = new DataHubPageTemplateSurface();
    surface.setSurfaceType(PageTemplateSurfaceType.HOME_PAGE);
    properties.setSurface(surface);

    // Set visibility
    DataHubPageTemplateVisibility visibility = new DataHubPageTemplateVisibility();
    visibility.setScope(PageTemplateScope.PERSONAL);
    properties.setVisibility(visibility);

    // Set audit stamps
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(UrnUtils.getUrn("urn:li:corpuser:test-user"));
    properties.setCreated(auditStamp);
    properties.setLastModified(auditStamp);

    return properties;
  }

  private List<DataHubPageTemplateRow> createTestRows() {
    DataHubPageTemplateRow row = new DataHubPageTemplateRow();
    row.setModules(
        new UrnArray(
            Arrays.asList(
                UrnUtils.getUrn("urn:li:dataHubPageModule:module1"),
                UrnUtils.getUrn("urn:li:dataHubPageModule:module2"))));
    return Collections.singletonList(row);
  }
}
