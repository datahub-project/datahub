package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.DOCUMENT_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SUB_TYPES_ASPECT_NAME;
import static com.linkedin.metadata.authorization.ApiOperation.CREATE;
import static com.linkedin.metadata.authorization.ApiOperation.DELETE;
import static com.linkedin.metadata.authorization.ApiOperation.READ;
import static com.linkedin.metadata.authorization.ApiOperation.UPDATE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.knowledge.DocumentInfo;
import com.linkedin.knowledge.RelatedAsset;
import com.linkedin.knowledge.RelatedAssetArray;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DocumentAuthorizationUtilsTest {

  private static final Urn STANDALONE_DOC = UrnUtils.getUrn("urn:li:document:standalone-doc");
  private static final Urn BRIDGE_DOC = UrnUtils.getUrn("urn:li:document:bridge-dataset-abc123");
  private static final Urn SECOND_BRIDGE_DOC =
      UrnUtils.getUrn("urn:li:document:bridge-dataset-def456");
  private static final Urn SOURCE_DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,bridge_src,PROD)");
  private static final Urn SECOND_SOURCE_DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,bridge_src_2,PROD)");

  private OperationContext opContext;
  private AspectRetriever aspectRetriever;
  private MockedStatic<AuthUtil> authUtilMock;

  @BeforeMethod
  public void setUp() {
    opContext = mock(OperationContext.class);
    aspectRetriever = mock(AspectRetriever.class);
    when(opContext.getAspectRetriever()).thenReturn(aspectRetriever);
    when(opContext.isSystemAuth()).thenReturn(false);
    authUtilMock = Mockito.mockStatic(AuthUtil.class);
  }

  @AfterMethod
  public void tearDown() {
    authUtilMock.close();
  }

  @Test
  public void testEffectiveDocumentIngestAuthorizationKey_usesExistenceToSelectPrivilege() {
    assertEquals(
        DocumentAuthorizationUtils.effectiveDocumentIngestAuthorizationKey(
            ChangeType.UPSERT, STANDALONE_DOC, false),
        Pair.of(ChangeType.CREATE_ENTITY, STANDALONE_DOC));
    assertEquals(
        DocumentAuthorizationUtils.effectiveDocumentIngestAuthorizationKey(
            ChangeType.UPDATE, STANDALONE_DOC, false),
        Pair.of(ChangeType.CREATE_ENTITY, STANDALONE_DOC));
    assertEquals(
        DocumentAuthorizationUtils.effectiveDocumentIngestAuthorizationKey(
            ChangeType.CREATE, STANDALONE_DOC, true),
        Pair.of(ChangeType.CREATE, STANDALONE_DOC));
    assertEquals(
        DocumentAuthorizationUtils.effectiveDocumentIngestAuthorizationKey(
            ChangeType.UPSERT, STANDALONE_DOC, true),
        Pair.of(ChangeType.UPSERT, STANDALONE_DOC));
    assertEquals(
        DocumentAuthorizationUtils.effectiveDocumentIngestAuthorizationKey(
            ChangeType.UPSERT, SOURCE_DATASET, false),
        Pair.of(ChangeType.UPSERT, SOURCE_DATASET));
  }

  @Test
  public void testIsUpdateLike() {
    assertTrue(DocumentAuthorizationUtils.isUpdateLike(ChangeType.CREATE));
    assertTrue(DocumentAuthorizationUtils.isUpdateLike(ChangeType.UPSERT));
    assertTrue(DocumentAuthorizationUtils.isUpdateLike(ChangeType.UPDATE));
    assertTrue(DocumentAuthorizationUtils.isUpdateLike(ChangeType.RESTATE));
    assertTrue(DocumentAuthorizationUtils.isUpdateLike(ChangeType.PATCH));
    assertFalse(DocumentAuthorizationUtils.isUpdateLike(ChangeType.DELETE));
  }

  @Test
  public void testIsAPIAuthorizedDocumentUrns_emptyAllows() {
    enableRestApiAuthorization();

    assertTrue(DocumentAuthorizationUtils.isAPIAuthorizedDocumentUrns(opContext, READ, List.of()));
  }

  @Test
  public void testIsAPIAuthorizedDocumentUrns_deleteUsesStandardEntityAuthorization() {
    enableRestApiAuthorization();
    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedEntityUrns(opContext, DELETE, List.of(STANDALONE_DOC)))
        .thenReturn(true);

    assertTrue(
        DocumentAuthorizationUtils.isAPIAuthorizedDocumentUrns(
            opContext, DELETE, List.of(STANDALONE_DOC)));
  }

  @Test
  public void testIsAPIAuthorizedDocumentUrns_mixedExistenceRequiresBothPrivileges() {
    enableRestApiAuthorization();
    when(aspectRetriever.entityExists(opContext, Set.of(STANDALONE_DOC, BRIDGE_DOC)))
        .thenReturn(Map.of(STANDALONE_DOC, true, BRIDGE_DOC, false));
    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedEntityUrns(opContext, UPDATE, List.of(STANDALONE_DOC)))
        .thenReturn(true);
    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedEntityUrns(opContext, CREATE, List.of(BRIDGE_DOC)))
        .thenReturn(true);

    assertTrue(
        DocumentAuthorizationUtils.isAPIAuthorizedDocumentUrns(
            opContext, UPDATE, List.of(STANDALONE_DOC, BRIDGE_DOC)));
  }

  @Test
  public void testIsAPIAuthorizedDocumentUrns_deniesUpdateOnMissingWithoutCreate() {
    enableRestApiAuthorization();
    when(aspectRetriever.entityExists(opContext, Set.of(STANDALONE_DOC)))
        .thenReturn(Map.of(STANDALONE_DOC, false));
    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedEntityUrns(opContext, CREATE, List.of(STANDALONE_DOC)))
        .thenReturn(false);

    assertFalse(
        DocumentAuthorizationUtils.isAPIAuthorizedDocumentUrns(
            opContext, UPDATE, List.of(STANDALONE_DOC)));
  }

  @Test
  public void testIsAPIAuthorizedDocumentUrns_deniesCreateOnExistingWithoutUpdate() {
    enableRestApiAuthorization();
    when(aspectRetriever.entityExists(opContext, Set.of(STANDALONE_DOC)))
        .thenReturn(Map.of(STANDALONE_DOC, true));
    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedEntityUrns(opContext, UPDATE, List.of(STANDALONE_DOC)))
        .thenReturn(false);

    assertFalse(
        DocumentAuthorizationUtils.isAPIAuthorizedDocumentUrns(
            opContext, CREATE, List.of(STANDALONE_DOC)));
  }

  @Test
  public void testCanViewStandaloneDocument_allowsWithEntityRead() {
    authUtilMock
        .when(
            () ->
                AuthUtil.isAuthorized(eq(opContext), eq(PoliciesConfig.MANAGE_DOCUMENTS_PRIVILEGE)))
        .thenReturn(false);
    authUtilMock
        .when(() -> AuthUtil.canViewEntity(eq(opContext), eq(STANDALONE_DOC)))
        .thenReturn(true);
    when(aspectRetriever.getLatestAspectObjects(any(), eq(Set.of(STANDALONE_DOC)), any()))
        .thenReturn(Map.of(STANDALONE_DOC, Map.of()));

    assertTrue(
        DocumentAuthorizationUtils.canViewDocumentEntity(
            opContext, aspectRetriever, STANDALONE_DOC));
  }

  @Test
  public void testCanViewStandaloneDocument_allowsWithManageDocuments() {
    authUtilMock
        .when(
            () ->
                AuthUtil.isAuthorized(eq(opContext), eq(PoliciesConfig.MANAGE_DOCUMENTS_PRIVILEGE)))
        .thenReturn(true);

    assertTrue(
        DocumentAuthorizationUtils.canViewDocumentEntity(
            opContext, aspectRetriever, STANDALONE_DOC));
  }

  @Test
  public void testCanViewStandaloneDocument_deniesWithoutPrivilege() {
    authUtilMock
        .when(
            () ->
                AuthUtil.isAuthorized(eq(opContext), eq(PoliciesConfig.MANAGE_DOCUMENTS_PRIVILEGE)))
        .thenReturn(false);
    authUtilMock
        .when(() -> AuthUtil.canViewEntity(eq(opContext), eq(STANDALONE_DOC)))
        .thenReturn(false);
    when(aspectRetriever.getLatestAspectObjects(any(), eq(Set.of(STANDALONE_DOC)), any()))
        .thenReturn(Map.of(STANDALONE_DOC, Map.of()));

    assertFalse(
        DocumentAuthorizationUtils.canViewDocumentEntity(
            opContext, aspectRetriever, STANDALONE_DOC));
  }

  @Test
  public void testCanViewNonDocumentDelegatesForBothOverloads() {
    AuthorizationSession session = mock(AuthorizationSession.class);
    authUtilMock.when(() -> AuthUtil.canViewEntity(session, SOURCE_DATASET)).thenReturn(true);
    authUtilMock.when(() -> AuthUtil.canViewEntity(opContext, SOURCE_DATASET)).thenReturn(true);

    assertTrue(
        DocumentAuthorizationUtils.canViewDocumentEntity(session, aspectRetriever, SOURCE_DATASET));
    assertTrue(
        DocumentAuthorizationUtils.canViewDocumentEntity(
            opContext, aspectRetriever, SOURCE_DATASET, null, null));
  }

  @Test
  public void testCanViewBridgeDocument_allowsViaSource() {
    authUtilMock
        .when(
            () ->
                AuthUtil.isAuthorized(eq(opContext), eq(PoliciesConfig.MANAGE_DOCUMENTS_PRIVILEGE)))
        .thenReturn(false);
    authUtilMock
        .when(() -> AuthUtil.canViewEntity(eq(opContext), eq(SOURCE_DATASET)))
        .thenReturn(true);
    when(aspectRetriever.getLatestAspectObjects(any(), eq(Set.of(BRIDGE_DOC)), any()))
        .thenReturn(Map.of(BRIDGE_DOC, bridgeAspects(SOURCE_DATASET)));

    assertTrue(
        DocumentAuthorizationUtils.canViewDocumentEntity(opContext, aspectRetriever, BRIDGE_DOC));
  }

  @Test
  public void testCanView_directReadSkipsAspectFetch() {
    authUtilMock
        .when(
            () ->
                AuthUtil.isAuthorized(eq(opContext), eq(PoliciesConfig.MANAGE_DOCUMENTS_PRIVILEGE)))
        .thenReturn(false);
    authUtilMock
        .when(() -> AuthUtil.canViewEntity(eq(opContext), eq(STANDALONE_DOC)))
        .thenReturn(true);

    assertTrue(
        DocumentAuthorizationUtils.canViewDocumentEntity(
            opContext, aspectRetriever, STANDALONE_DOC));
    Mockito.verify(aspectRetriever, Mockito.never()).getLatestAspectObjects(any(), any(), any());
  }

  @Test
  public void testCanViewBridgeDocument_allowsViaDocumentEvenWhenSourceUnresolvable() {
    authUtilMock
        .when(
            () ->
                AuthUtil.isAuthorized(eq(opContext), eq(PoliciesConfig.MANAGE_DOCUMENTS_PRIVILEGE)))
        .thenReturn(false);
    authUtilMock.when(() -> AuthUtil.canViewEntity(eq(opContext), eq(BRIDGE_DOC))).thenReturn(true);

    assertTrue(
        DocumentAuthorizationUtils.canViewDocumentEntity(
            opContext, aspectRetriever, BRIDGE_DOC, unresolvableBridgeInfo(), bridgeSubTypes()));
  }

  @Test
  public void testCanViewBridgeDocument_allowsViaDocument() {
    authUtilMock
        .when(
            () ->
                AuthUtil.isAuthorized(eq(opContext), eq(PoliciesConfig.MANAGE_DOCUMENTS_PRIVILEGE)))
        .thenReturn(false);
    authUtilMock.when(() -> AuthUtil.canViewEntity(eq(opContext), eq(BRIDGE_DOC))).thenReturn(true);
    authUtilMock
        .when(() -> AuthUtil.canViewEntity(eq(opContext), eq(SOURCE_DATASET)))
        .thenReturn(false);

    assertTrue(
        DocumentAuthorizationUtils.canViewDocumentEntity(
            opContext,
            aspectRetriever,
            BRIDGE_DOC,
            bridgeDocumentInfo(SOURCE_DATASET),
            bridgeSubTypes()));
  }

  @Test
  public void testCanViewBridgeDocument_deniesWhenSourceUnresolvable() {
    authUtilMock
        .when(
            () ->
                AuthUtil.isAuthorized(eq(opContext), eq(PoliciesConfig.MANAGE_DOCUMENTS_PRIVILEGE)))
        .thenReturn(false);
    DocumentInfo info = new DocumentInfo();
    StringMap props = new StringMap();
    props.put(DocumentAuthorizationUtils.BRIDGE_TYPE_PROPERTY, "dataset");
    // missing bridge_source_entity
    info.setCustomProperties(props);
    SubTypes subTypes = new SubTypes();
    subTypes.setTypeNames(new StringArray(DocumentAuthorizationUtils.BRIDGE_DOCUMENT_SUBTYPE));
    when(aspectRetriever.getLatestAspectObjects(any(), eq(Set.of(BRIDGE_DOC)), any()))
        .thenReturn(
            Map.of(
                BRIDGE_DOC,
                Map.of(
                    DOCUMENT_INFO_ASPECT_NAME, new Aspect(info.data()),
                    SUB_TYPES_ASPECT_NAME, new Aspect(subTypes.data()))));

    assertFalse(
        DocumentAuthorizationUtils.canViewDocumentEntity(opContext, aspectRetriever, BRIDGE_DOC));
  }

  @Test
  public void testCanView_systemAuthAllows() {
    when(opContext.isSystemAuth()).thenReturn(true);
    assertTrue(
        DocumentAuthorizationUtils.canViewDocumentEntity(
            opContext, aspectRetriever, STANDALONE_DOC));
  }

  @Test
  public void testRelatedAssetsDoNotGrantViewOnNonBridge() {
    authUtilMock
        .when(
            () ->
                AuthUtil.isAuthorized(eq(opContext), eq(PoliciesConfig.MANAGE_DOCUMENTS_PRIVILEGE)))
        .thenReturn(false);
    authUtilMock
        .when(() -> AuthUtil.canViewEntity(eq(opContext), eq(STANDALONE_DOC)))
        .thenReturn(false);
    DocumentInfo info = new DocumentInfo();
    // Non-bridge doc with related asset — must not inherit VIEW from the asset.
    when(aspectRetriever.getLatestAspectObjects(any(), eq(Set.of(STANDALONE_DOC)), any()))
        .thenReturn(
            Map.of(STANDALONE_DOC, Map.of(DOCUMENT_INFO_ASPECT_NAME, new Aspect(info.data()))));

    assertFalse(
        DocumentAuthorizationUtils.canViewDocumentEntity(
            opContext, aspectRetriever, STANDALONE_DOC));
  }

  @Test
  public void testIsBridgeDocument() {
    SubTypes subTypes = new SubTypes();
    subTypes.setTypeNames(new StringArray(DocumentAuthorizationUtils.BRIDGE_DOCUMENT_SUBTYPE));
    assertTrue(DocumentAuthorizationUtils.isBridgeDocument(BRIDGE_DOC, subTypes));
    assertFalse(DocumentAuthorizationUtils.isBridgeDocument(STANDALONE_DOC, subTypes));
    assertFalse(DocumentAuthorizationUtils.isBridgeDocument(SOURCE_DATASET, subTypes));
    assertFalse(DocumentAuthorizationUtils.isBridgeDocument(BRIDGE_DOC, null));
    assertFalse(DocumentAuthorizationUtils.isBridgeDocument(BRIDGE_DOC, new SubTypes()));
    assertFalse(
        DocumentAuthorizationUtils.isBridgeDocument(
            BRIDGE_DOC, new SubTypes().setTypeNames(new StringArray("Other"))));
  }

  @Test
  public void testResolveDocumentBridgeSourceUrn_rejectsIncompleteBridgeMetadata() {
    assertNull(
        DocumentAuthorizationUtils.resolveDocumentBridgeSourceUrn(
            BRIDGE_DOC, null, bridgeSubTypes()));
    assertNull(
        DocumentAuthorizationUtils.resolveDocumentBridgeSourceUrn(
            BRIDGE_DOC, new DocumentInfo(), bridgeSubTypes()));

    DocumentInfo missingBridgeType = new DocumentInfo();
    missingBridgeType.setCustomProperties(
        new StringMap(
            Map.of(
                DocumentAuthorizationUtils.BRIDGE_SOURCE_ENTITY_PROPERTY,
                SOURCE_DATASET.toString())));
    assertNull(
        DocumentAuthorizationUtils.resolveDocumentBridgeSourceUrn(
            BRIDGE_DOC, missingBridgeType, bridgeSubTypes()));

    assertNull(
        DocumentAuthorizationUtils.resolveDocumentBridgeSourceUrn(
            STANDALONE_DOC, bridgeDocumentInfo(SOURCE_DATASET), bridgeSubTypes()));
  }

  @Test
  public void testResolveDocumentBridgeSourceUrn_rejectsMismatchedAndMalformedSources() {
    DocumentInfo mismatchedType = bridgeDocumentInfo(SOURCE_DATASET);
    mismatchedType
        .getCustomProperties()
        .put(DocumentAuthorizationUtils.BRIDGE_TYPE_PROPERTY, "chart");
    assertNull(
        DocumentAuthorizationUtils.resolveDocumentBridgeSourceUrn(
            BRIDGE_DOC, mismatchedType, bridgeSubTypes()));

    DocumentInfo malformedSource = bridgeDocumentInfo(SOURCE_DATASET);
    malformedSource
        .getCustomProperties()
        .put(DocumentAuthorizationUtils.BRIDGE_SOURCE_ENTITY_PROPERTY, "not-a-urn");
    assertNull(
        DocumentAuthorizationUtils.resolveDocumentBridgeSourceUrn(
            BRIDGE_DOC, malformedSource, bridgeSubTypes()));
  }

  @Test
  public void testResolveDocumentBridgeSourceUrn_fallsBackToLegacyRelatedDataset() {
    DocumentInfo info = new DocumentInfo();
    info.setCustomProperties(
        new StringMap(Map.of(DocumentAuthorizationUtils.BRIDGE_TYPE_PROPERTY, "dataset")));
    RelatedAsset nonDataset =
        new RelatedAsset().setAsset(UrnUtils.getUrn("urn:li:corpuser:not-a-dataset"));
    RelatedAsset dataset = new RelatedAsset().setAsset(SOURCE_DATASET);
    info.setRelatedAssets(new RelatedAssetArray(nonDataset, dataset));

    assertEquals(
        DocumentAuthorizationUtils.resolveDocumentBridgeSourceUrn(
            BRIDGE_DOC, info, bridgeSubTypes()),
        SOURCE_DATASET);
  }

  @Test
  public void testCanViewDocumentEntity_preloadedDoesNotFetchAspects() {
    authUtilMock
        .when(
            () ->
                AuthUtil.isAuthorized(eq(opContext), eq(PoliciesConfig.MANAGE_DOCUMENTS_PRIVILEGE)))
        .thenReturn(false);
    authUtilMock
        .when(() -> AuthUtil.canViewEntity(eq(opContext), eq(SOURCE_DATASET)))
        .thenReturn(true);

    assertTrue(
        DocumentAuthorizationUtils.canViewDocumentEntity(
            opContext,
            aspectRetriever,
            BRIDGE_DOC,
            bridgeDocumentInfo(SOURCE_DATASET),
            bridgeSubTypes()));
    Mockito.verify(aspectRetriever, Mockito.never()).getLatestAspectObjects(any(), any(), any());
  }

  @Test
  public void testIsAPIAuthorizedDocumentUrns_bridgeAllowedViaDocumentView() {
    authUtilMock.when(AuthUtil::isRestApiAuthorizationEnabled).thenReturn(true);
    authUtilMock
        .when(
            () ->
                AuthUtil.isAuthorized(eq(opContext), eq(PoliciesConfig.MANAGE_DOCUMENTS_PRIVILEGE)))
        .thenReturn(false);
    authUtilMock.when(() -> AuthUtil.canViewEntity(eq(opContext), eq(BRIDGE_DOC))).thenReturn(true);
    when(aspectRetriever.getLatestAspectObjects(any(), eq(Set.of(BRIDGE_DOC)), any()))
        .thenReturn(Map.of(BRIDGE_DOC, bridgeAspects(SOURCE_DATASET)));

    assertTrue(
        DocumentAuthorizationUtils.isAPIAuthorizedDocumentUrns(
            opContext, READ, List.of(BRIDGE_DOC)));
  }

  @Test
  public void testIsAPIAuthorizedDocumentUrns_batchesBridgeAspectFetches() {
    enableRestApiAuthorization();
    authUtilMock
        .when(
            () ->
                AuthUtil.isAuthorized(eq(opContext), eq(PoliciesConfig.MANAGE_DOCUMENTS_PRIVILEGE)))
        .thenReturn(false);
    authUtilMock
        .when(() -> AuthUtil.canViewEntity(eq(opContext), eq(SOURCE_DATASET)))
        .thenReturn(true);
    authUtilMock
        .when(() -> AuthUtil.canViewEntity(eq(opContext), eq(SECOND_SOURCE_DATASET)))
        .thenReturn(true);
    Set<Urn> bridgeDocuments = Set.of(BRIDGE_DOC, SECOND_BRIDGE_DOC);
    when(aspectRetriever.getLatestAspectObjects(
            opContext, bridgeDocuments, Set.of(DOCUMENT_INFO_ASPECT_NAME, SUB_TYPES_ASPECT_NAME)))
        .thenReturn(
            Map.of(
                BRIDGE_DOC,
                bridgeAspects(SOURCE_DATASET),
                SECOND_BRIDGE_DOC,
                bridgeAspects(SECOND_SOURCE_DATASET)));

    assertTrue(
        DocumentAuthorizationUtils.isAPIAuthorizedDocumentUrns(
            opContext, READ, List.of(BRIDGE_DOC, SECOND_BRIDGE_DOC)));
    verify(aspectRetriever)
        .getLatestAspectObjects(
            opContext, bridgeDocuments, Set.of(DOCUMENT_INFO_ASPECT_NAME, SUB_TYPES_ASPECT_NAME));
  }

  @Test
  public void testCanViewBridgeDocument_unresolvableIncrementsMetric() {
    authUtilMock
        .when(
            () ->
                AuthUtil.isAuthorized(eq(opContext), eq(PoliciesConfig.MANAGE_DOCUMENTS_PRIVILEGE)))
        .thenReturn(false);
    MetricUtils metricUtils = mock(MetricUtils.class);
    when(opContext.getMetricUtils()).thenReturn(Optional.of(metricUtils));

    assertFalse(
        DocumentAuthorizationUtils.canViewDocumentEntity(
            opContext, aspectRetriever, BRIDGE_DOC, unresolvableBridgeInfo(), bridgeSubTypes()));
    verify(metricUtils)
        .increment(
            DocumentAuthorizationUtils.class,
            DocumentAuthorizationUtils.BRIDGE_SOURCE_RESOLUTION_FAILED_METRIC,
            1);
  }

  @Test
  public void testIsAPIAuthorizedDocumentUrns_restApiDisabledAllows() {
    authUtilMock.when(AuthUtil::isRestApiAuthorizationEnabled).thenReturn(false);
    assertTrue(
        DocumentAuthorizationUtils.isAPIAuthorizedDocumentUrns(
            opContext, READ, List.of(STANDALONE_DOC)));
  }

  @Test
  public void testIsAPIAuthorizedDocumentUrns_updateUsesCreatePrivilegeForMissingDocument() {
    enableRestApiAuthorization();
    when(aspectRetriever.entityExists(opContext, Set.of(STANDALONE_DOC)))
        .thenReturn(Map.of(STANDALONE_DOC, false));
    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedEntityUrns(opContext, CREATE, List.of(STANDALONE_DOC)))
        .thenReturn(true);

    assertTrue(
        DocumentAuthorizationUtils.isAPIAuthorizedDocumentUrns(
            opContext, UPDATE, List.of(STANDALONE_DOC)));
    authUtilMock.verify(
        () -> AuthUtil.isAPIAuthorizedEntityUrns(opContext, CREATE, List.of(STANDALONE_DOC)));
  }

  @Test
  public void testIsAPIAuthorizedDocumentUrns_createUsesUpdatePrivilegeForExistingDocument() {
    enableRestApiAuthorization();
    when(aspectRetriever.entityExists(opContext, Set.of(STANDALONE_DOC)))
        .thenReturn(Map.of(STANDALONE_DOC, true));
    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedEntityUrns(opContext, UPDATE, List.of(STANDALONE_DOC)))
        .thenReturn(true);

    assertTrue(
        DocumentAuthorizationUtils.isAPIAuthorizedDocumentUrns(
            opContext, CREATE, List.of(STANDALONE_DOC)));
    authUtilMock.verify(
        () -> AuthUtil.isAPIAuthorizedEntityUrns(opContext, UPDATE, List.of(STANDALONE_DOC)));
  }

  @Test
  public void testIsAuthorizedDocumentOperation_bridgeSourceDoesNotAllowUpdate() {
    authUtilMock
        .when(() -> AuthUtil.isAuthorizedEntityUrns(opContext, UPDATE, List.of(BRIDGE_DOC)))
        .thenReturn(false);
    authUtilMock
        .when(() -> AuthUtil.isAuthorized(opContext, PoliciesConfig.MANAGE_DOCUMENTS_PRIVILEGE))
        .thenReturn(false);
    // Source access must not grant document UPDATE / DELETE inheritance.
    authUtilMock
        .when(() -> AuthUtil.canViewEntity(eq(opContext), eq(SOURCE_DATASET)))
        .thenReturn(true);

    assertFalse(
        DocumentAuthorizationUtils.isAuthorizedDocumentOperation(opContext, UPDATE, BRIDGE_DOC));
    assertFalse(
        DocumentAuthorizationUtils.isAuthorizedDocumentOperation(opContext, DELETE, BRIDGE_DOC));
  }

  @Test
  public void testIsAuthorizedDocumentOperation_systemAndEmptyAllow() {
    when(opContext.isSystemAuth()).thenReturn(true);
    assertTrue(
        DocumentAuthorizationUtils.isAuthorizedDocumentOperation(opContext, UPDATE, BRIDGE_DOC));

    when(opContext.isSystemAuth()).thenReturn(false);
    assertTrue(
        DocumentAuthorizationUtils.isAuthorizedDocumentOperation(opContext, UPDATE, List.of()));
  }

  @Test
  public void testIsAuthorizedDocumentOperation_readUsesDocumentView() {
    authUtilMock
        .when(
            () ->
                AuthUtil.isAuthorized(eq(opContext), eq(PoliciesConfig.MANAGE_DOCUMENTS_PRIVILEGE)))
        .thenReturn(false);
    authUtilMock
        .when(() -> AuthUtil.canViewEntity(eq(opContext), eq(STANDALONE_DOC)))
        .thenReturn(false);
    when(aspectRetriever.getLatestAspectObjects(any(), eq(Set.of(STANDALONE_DOC)), any()))
        .thenReturn(Map.of(STANDALONE_DOC, Map.of()));

    assertFalse(
        DocumentAuthorizationUtils.isAuthorizedDocumentOperation(opContext, READ, STANDALONE_DOC));
  }

  @Test
  public void testIsAuthorizedDocumentOperation_updateUsesEntityOrManagePrivilege() {
    authUtilMock
        .when(() -> AuthUtil.isAuthorizedEntityUrns(opContext, UPDATE, List.of(STANDALONE_DOC)))
        .thenReturn(true);
    assertTrue(
        DocumentAuthorizationUtils.isAuthorizedDocumentOperation(
            opContext, UPDATE, STANDALONE_DOC));

    authUtilMock
        .when(() -> AuthUtil.isAuthorizedEntityUrns(opContext, UPDATE, List.of(STANDALONE_DOC)))
        .thenReturn(false);
    authUtilMock
        .when(() -> AuthUtil.isAuthorized(opContext, PoliciesConfig.MANAGE_DOCUMENTS_PRIVILEGE))
        .thenReturn(true);
    assertTrue(
        DocumentAuthorizationUtils.isAuthorizedDocumentOperation(
            opContext, UPDATE, STANDALONE_DOC));
  }

  @Test
  public void testAssertAuthorizedDocumentOperation_deniedThrows() {
    authUtilMock
        .when(() -> AuthUtil.isAuthorizedEntityUrns(opContext, UPDATE, List.of(STANDALONE_DOC)))
        .thenReturn(false);
    authUtilMock
        .when(() -> AuthUtil.isAuthorized(opContext, PoliciesConfig.MANAGE_DOCUMENTS_PRIVILEGE))
        .thenReturn(false);

    assertThrows(
        ServiceAuthorizationException.class,
        () ->
            DocumentAuthorizationUtils.assertAuthorizedDocumentOperation(
                opContext, UPDATE, STANDALONE_DOC));
  }

  private void enableRestApiAuthorization() {
    authUtilMock.when(AuthUtil::isRestApiAuthorizationEnabled).thenReturn(true);
  }

  private static DocumentInfo unresolvableBridgeInfo() {
    DocumentInfo info = new DocumentInfo();
    StringMap props = new StringMap();
    props.put(DocumentAuthorizationUtils.BRIDGE_TYPE_PROPERTY, "dataset");
    info.setCustomProperties(props);
    return info;
  }

  private static DocumentInfo bridgeDocumentInfo(Urn sourceUrn) {
    DocumentInfo info = new DocumentInfo();
    StringMap props = new StringMap();
    props.put(DocumentAuthorizationUtils.BRIDGE_TYPE_PROPERTY, sourceUrn.getEntityType());
    props.put(DocumentAuthorizationUtils.BRIDGE_SOURCE_ENTITY_PROPERTY, sourceUrn.toString());
    info.setCustomProperties(props);
    return info;
  }

  private static SubTypes bridgeSubTypes() {
    SubTypes subTypes = new SubTypes();
    subTypes.setTypeNames(new StringArray(DocumentAuthorizationUtils.BRIDGE_DOCUMENT_SUBTYPE));
    return subTypes;
  }

  private static Map<String, Aspect> bridgeAspects(Urn sourceUrn) {
    return Map.of(
        DOCUMENT_INFO_ASPECT_NAME, new Aspect(bridgeDocumentInfo(sourceUrn).data()),
        SUB_TYPES_ASPECT_NAME, new Aspect(bridgeSubTypes().data()));
  }
}
