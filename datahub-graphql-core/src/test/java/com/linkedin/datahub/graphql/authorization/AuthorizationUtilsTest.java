package com.linkedin.datahub.graphql.authorization;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ViewProperties;
import com.linkedin.knowledge.DocumentInfo;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.authorization.EntityAuthorizationUtils;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.service.DocumentAuthorizationUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import java.util.Map;
import java.util.Set;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class AuthorizationUtilsTest {

  private static final Urn TEST_DOCUMENT_URN = UrnUtils.getUrn("urn:li:document:test-doc");
  private static final Urn TEST_BRIDGE_DOCUMENT_URN =
      UrnUtils.getUrn("urn:li:document:bridge-dataset-graphql-test");
  private static final Urn TEST_SOURCE_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:testPlatform,bridge-source,PROD)");
  private static final Urn TEST_SCHEMA_FIELD_URN =
      UrnUtils.getUrn(
          "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD),field_foo)");

  @Test
  public void testRestrictedViewProperties() {
    // provides a test of primitive boolean
    ViewProperties viewProperties =
        ViewProperties.builder()
            .setMaterialized(true)
            .setLanguage("testLang")
            .setFormattedLogic("formattedLogic")
            .setLogic("testLogic")
            .build();

    String expected =
        ViewProperties.builder()
            .setMaterialized(true)
            .setLanguage("")
            .setLogic("")
            .build()
            .toString();

    assertEquals(
        AuthorizationUtils.restrictEntity(viewProperties, ViewProperties.class).toString(),
        expected);
  }

  @Test
  public void testCanCreateDocument() {
    QueryContext mockContext = getMockAllowContext();
    assertTrue(AuthorizationUtils.canCreateDocument(mockContext));
  }

  @Test
  public void testCanCreateDocumentWithDenyContext() {
    QueryContext mockContext = getMockDenyContext();
    assertFalse(AuthorizationUtils.canCreateDocument(mockContext));
  }

  @Test
  public void testCanEditDocumentAuthorized() {
    QueryContext mockContext = getMockAllowContext();
    assertTrue(AuthorizationUtils.canEditDocument(TEST_DOCUMENT_URN, mockContext));
  }

  @Test
  public void testCanEditDocumentWithDenyContext() {
    QueryContext mockContext = getMockDenyContext();
    assertFalse(AuthorizationUtils.canEditDocument(TEST_DOCUMENT_URN, mockContext));
  }

  @Test
  public void testCanGetDocumentAuthorized() {
    QueryContext mockContext = getMockAllowContext();
    assertTrue(AuthorizationUtils.canGetDocument(TEST_DOCUMENT_URN, mockContext));
  }

  @Test
  public void testCanGetDocumentWithDenyContext() {
    QueryContext mockContext = getMockDenyContext();
    assertFalse(AuthorizationUtils.canGetDocument(TEST_DOCUMENT_URN, mockContext));
  }

  @Test
  public void testCanDeleteDocumentAuthorized() {
    QueryContext mockContext = getMockAllowContext();
    assertTrue(AuthorizationUtils.canDeleteDocument(TEST_DOCUMENT_URN, mockContext));
  }

  @Test
  public void testCanDeleteDocumentWithDenyContext() {
    QueryContext mockContext = getMockDenyContext();
    assertFalse(AuthorizationUtils.canDeleteDocument(TEST_DOCUMENT_URN, mockContext));
  }

  @Test
  public void testCanManageDocuments() {
    QueryContext mockContext = getMockAllowContext();
    assertTrue(AuthorizationUtils.canManageDocuments(mockContext));
  }

  @Test
  public void testCanCreateLogicalModelsAllowed() {
    QueryContext context = getMockAllowContext();
    assertTrue(AuthorizationUtils.canCreateLogicalModels(context));
  }

  @Test
  public void testCanCreateLogicalModelsDenied() {
    QueryContext context = getMockDenyContext();
    assertFalse(AuthorizationUtils.canCreateLogicalModels(context));
  }

  @DataProvider
  public Object[][] bridgeDocumentAuthorizationCases() {
    return new Object[][] {
      {"document policy", true, false, true},
      {"source policy", false, true, true},
      {"no matching policy", false, false, false}
    };
  }

  @Test(dataProvider = "bridgeDocumentAuthorizationCases")
  public void testCanViewBridgeDocumentAuthorizationWhenViewAuthorizationEnabled(
      String caseName, boolean canViewDocument, boolean canViewSource, boolean expectedAllowed) {
    OperationContext opContext = mock(OperationContext.class);
    OperationContextConfig config = mock(OperationContextConfig.class);
    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    ViewAuthorizationConfiguration viewAuth =
        ViewAuthorizationConfiguration.builder().enabled(true).build();
    when(opContext.getOperationContextConfig()).thenReturn(config);
    when(config.getViewAuthorizationConfiguration()).thenReturn(viewAuth);
    when(opContext.getAspectRetriever()).thenReturn(aspectRetriever);
    when(opContext.isSystemAuth()).thenReturn(false);

    DocumentInfo documentInfo = bridgeDocumentInfo();
    SubTypes subTypes = bridgeSubTypes();

    try (MockedStatic<AuthUtil> authUtil =
        Mockito.mockStatic(AuthUtil.class, Mockito.CALLS_REAL_METHODS)) {
      authUtil
          .when(() -> AuthUtil.isViewRestrictedEntityType(eq(viewAuth), eq("document")))
          .thenReturn(true);
      stubDocumentPrivileges(authUtil, opContext, canViewDocument, canViewSource);

      assertEquals(
          AuthorizationUtils.canViewDocument(
              opContext, TEST_BRIDGE_DOCUMENT_URN, documentInfo, subTypes),
          expectedAllowed,
          caseName);
    }
  }

  @Test
  public void testCanViewDocumentSkipsPrivilegeCheckWhenViewAuthorizationDisabled() {
    OperationContext opContext = mock(OperationContext.class);
    OperationContextConfig config = mock(OperationContextConfig.class);
    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    ViewAuthorizationConfiguration viewAuth =
        ViewAuthorizationConfiguration.builder().enabled(false).build();
    when(opContext.getOperationContextConfig()).thenReturn(config);
    when(config.getViewAuthorizationConfiguration()).thenReturn(viewAuth);
    when(opContext.getAspectRetriever()).thenReturn(aspectRetriever);
    when(opContext.isSystemAuth()).thenReturn(false);

    try (MockedStatic<AuthUtil> authUtil =
        Mockito.mockStatic(AuthUtil.class, Mockito.CALLS_REAL_METHODS)) {
      stubDocumentPrivileges(authUtil, opContext, false, false);

      assertTrue(
          AuthorizationUtils.canViewDocument(
              opContext, TEST_BRIDGE_DOCUMENT_URN, bridgeDocumentInfo(), bridgeSubTypes()));
      authUtil.verify(
          () -> AuthUtil.canViewEntity(eq(opContext), eq(TEST_BRIDGE_DOCUMENT_URN)),
          Mockito.never());
    }
  }

  @Test
  public void testCanGetDocumentEvaluatesPrivilegesWhenViewAuthorizationDisabled() {
    QueryContext context = mock(QueryContext.class);
    OperationContext opContext = mock(OperationContext.class);
    OperationContextConfig config = mock(OperationContextConfig.class);
    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    ViewAuthorizationConfiguration viewAuth =
        ViewAuthorizationConfiguration.builder().enabled(false).build();
    when(context.getOperationContext()).thenReturn(opContext);
    when(opContext.getOperationContextConfig()).thenReturn(config);
    when(config.getViewAuthorizationConfiguration()).thenReturn(viewAuth);
    when(opContext.getAspectRetriever()).thenReturn(aspectRetriever);
    when(opContext.isSystemAuth()).thenReturn(false);
    when(aspectRetriever.getLatestAspectObjects(any(), eq(Set.of(TEST_DOCUMENT_URN)), any()))
        .thenReturn(Map.of(TEST_DOCUMENT_URN, Map.of()));

    try (MockedStatic<AuthUtil> authUtil =
        Mockito.mockStatic(AuthUtil.class, Mockito.CALLS_REAL_METHODS)) {
      authUtil
          .when(
              () ->
                  AuthUtil.isAuthorized(
                      eq(opContext), eq(PoliciesConfig.MANAGE_DOCUMENTS_PRIVILEGE)))
          .thenReturn(false);
      authUtil
          .when(() -> AuthUtil.canViewEntity(eq(opContext), eq(TEST_DOCUMENT_URN)))
          .thenReturn(false);

      assertFalse(AuthorizationUtils.canGetDocument(TEST_DOCUMENT_URN, context));
    }
  }

  private static DocumentInfo bridgeDocumentInfo() {
    DocumentInfo documentInfo = new DocumentInfo();
    StringMap properties = new StringMap();
    properties.put(
        DocumentAuthorizationUtils.BRIDGE_TYPE_PROPERTY, TEST_SOURCE_URN.getEntityType());
    properties.put(
        DocumentAuthorizationUtils.BRIDGE_SOURCE_ENTITY_PROPERTY, TEST_SOURCE_URN.toString());
    documentInfo.setCustomProperties(properties);
    return documentInfo;
  }

  private static SubTypes bridgeSubTypes() {
    return new SubTypes()
        .setTypeNames(new StringArray(DocumentAuthorizationUtils.BRIDGE_DOCUMENT_SUBTYPE));
  }

  private static void stubDocumentPrivileges(
      MockedStatic<AuthUtil> authUtil,
      OperationContext opContext,
      boolean canViewDocument,
      boolean canViewSource) {
    authUtil
        .when(
            () ->
                AuthUtil.isAuthorized(eq(opContext), eq(PoliciesConfig.MANAGE_DOCUMENTS_PRIVILEGE)))
        .thenReturn(false);
    authUtil
        .when(() -> AuthUtil.canViewEntity(eq(opContext), eq(TEST_BRIDGE_DOCUMENT_URN)))
        .thenReturn(canViewDocument);
    authUtil
        .when(() -> AuthUtil.canViewEntity(eq(opContext), eq(TEST_SOURCE_URN)))
        .thenReturn(canViewSource);
  }

  @Test
  public void testCanViewSchemaFieldUsesEntityAuthorizationFacade() {
    OperationContext opContext = mock(OperationContext.class);
    OperationContextConfig config = mock(OperationContextConfig.class);
    ViewAuthorizationConfiguration viewAuth =
        ViewAuthorizationConfiguration.builder().enabled(true).build();
    when(opContext.getOperationContextConfig()).thenReturn(config);
    when(config.getViewAuthorizationConfiguration()).thenReturn(viewAuth);
    when(opContext.isSystemAuth()).thenReturn(false);

    try (MockedStatic<AuthUtil> authUtil =
            Mockito.mockStatic(AuthUtil.class, Mockito.CALLS_REAL_METHODS);
        MockedStatic<EntityAuthorizationUtils> entityAuth =
            Mockito.mockStatic(EntityAuthorizationUtils.class)) {
      authUtil
          .when(() -> AuthUtil.isViewRestrictedEntityType(eq(viewAuth), eq("schemaField")))
          .thenReturn(true);
      entityAuth
          .when(
              () ->
                  EntityAuthorizationUtils.canViewEntity(eq(opContext), eq(TEST_SCHEMA_FIELD_URN)))
          .thenReturn(true);

      assertTrue(AuthorizationUtils.canView(opContext, TEST_SCHEMA_FIELD_URN));
      entityAuth.verify(
          () -> EntityAuthorizationUtils.canViewEntity(opContext, TEST_SCHEMA_FIELD_URN));
    }
  }
}
