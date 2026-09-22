package com.linkedin.metadata.authorization;

import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;
import static com.linkedin.metadata.authorization.ApiOperation.CREATE;
import static com.linkedin.metadata.authorization.ApiOperation.READ;
import static com.linkedin.metadata.authorization.ApiOperation.UPDATE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.AuthUtil;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.browse.BrowseResultEntity;
import com.linkedin.metadata.browse.BrowseResultEntityArray;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.AutoCompleteEntity;
import com.linkedin.metadata.query.AutoCompleteEntityArray;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.search.LineageScrollResult;
import com.linkedin.metadata.search.LineageSearchEntity;
import com.linkedin.metadata.search.LineageSearchEntityArray;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.DocumentAuthorizationUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class EntityAuthorizationUtilsTest {

  private static final Urn DOCUMENT_URN = UrnUtils.getUrn("urn:li:document:facade-doc");
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,facade,PROD)");
  private static final Urn QUERY_URN = UrnUtils.getUrn("urn:li:query:facade-query");
  private static final Urn SCHEMA_FIELD_URN =
      UrnUtils.getUrn(
          "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,facade,PROD),field_foo)");

  private OperationContext opContext;
  private AspectRetriever aspectRetriever;
  private MockedStatic<AuthUtil> authUtilMock;
  private MockedStatic<DocumentAuthorizationUtils> documentAuthMock;

  @BeforeMethod
  public void setUp() {
    opContext = mock(OperationContext.class);
    aspectRetriever = mock(AspectRetriever.class);
    when(opContext.getAspectRetriever()).thenReturn(aspectRetriever);
    when(opContext.isSystemAuth()).thenReturn(false);
    authUtilMock = Mockito.mockStatic(AuthUtil.class);
    documentAuthMock = Mockito.mockStatic(DocumentAuthorizationUtils.class);
    documentAuthMock
        .when(() -> DocumentAuthorizationUtils.isDocumentEntity(DOCUMENT_URN))
        .thenReturn(true);
    documentAuthMock
        .when(() -> DocumentAuthorizationUtils.isDocumentEntity(DATASET_URN))
        .thenReturn(false);
    documentAuthMock
        .when(() -> DocumentAuthorizationUtils.isDocumentEntity(SCHEMA_FIELD_URN))
        .thenReturn(false);
    documentAuthMock
        .when(() -> DocumentAuthorizationUtils.isDocumentEntity(QUERY_URN))
        .thenReturn(false);
  }

  @AfterMethod
  public void tearDown() {
    documentAuthMock.close();
    authUtilMock.close();
  }

  @Test
  public void testIsAPIAuthorizedEntityUrns_routesDocumentsAndOthers() {
    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedEntityUrns(opContext, READ, List.of(DATASET_URN)))
        .thenReturn(true);
    documentAuthMock
        .when(
            () ->
                DocumentAuthorizationUtils.isAPIAuthorizedDocumentUrns(
                    opContext, READ, List.of(DOCUMENT_URN)))
        .thenReturn(true);

    assertTrue(
        EntityAuthorizationUtils.isAPIAuthorizedEntityUrns(
            opContext, READ, List.of(DATASET_URN, DOCUMENT_URN)));

    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedEntityUrns(opContext, READ, List.of(DATASET_URN)))
        .thenReturn(false);
    assertFalse(
        EntityAuthorizationUtils.isAPIAuthorizedEntityUrns(
            opContext, READ, List.of(DATASET_URN, DOCUMENT_URN)));
  }

  @Test
  public void testIsAPIAuthorizedEntityUrns_nonDocumentsOnlyUsesStandardAuthorization() {
    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedEntityUrns(opContext, READ, List.of(DATASET_URN)))
        .thenReturn(true);

    assertTrue(
        EntityAuthorizationUtils.isAPIAuthorizedEntityUrns(opContext, READ, List.of(DATASET_URN)));
  }

  @Test
  public void testIsAPIAuthorizedEntityUrns_schemaFieldReadAllowedViaParentInheritance() {
    authUtilMock.when(AuthUtil::isRestApiAuthorizationEnabled).thenReturn(true);
    try (MockedStatic<EntityAspectAuthorizationUtils> schemaFieldAuth =
        Mockito.mockStatic(EntityAspectAuthorizationUtils.class, Mockito.CALLS_REAL_METHODS)) {
      schemaFieldAuth
          .when(() -> EntityAspectAuthorizationUtils.isSchemaFieldEntity(SCHEMA_FIELD_URN))
          .thenReturn(true);
      schemaFieldAuth
          .when(
              () ->
                  EntityAspectAuthorizationUtils.canViewSchemaFieldEntity(
                      opContext, SCHEMA_FIELD_URN))
          .thenReturn(true);

      assertTrue(
          EntityAuthorizationUtils.isAPIAuthorizedEntityUrns(
              opContext, READ, List.of(SCHEMA_FIELD_URN)));
      authUtilMock.verify(
          () -> AuthUtil.isAPIAuthorizedEntityUrns(opContext, READ, List.of(SCHEMA_FIELD_URN)),
          Mockito.never());
    }
  }

  @Test
  public void testIsAPIAuthorizedEntityUrns_schemaFieldReadDeniedWithoutGrant() {
    authUtilMock.when(AuthUtil::isRestApiAuthorizationEnabled).thenReturn(true);
    try (MockedStatic<EntityAspectAuthorizationUtils> schemaFieldAuth =
        Mockito.mockStatic(EntityAspectAuthorizationUtils.class, Mockito.CALLS_REAL_METHODS)) {
      schemaFieldAuth
          .when(() -> EntityAspectAuthorizationUtils.isSchemaFieldEntity(SCHEMA_FIELD_URN))
          .thenReturn(true);
      schemaFieldAuth
          .when(
              () ->
                  EntityAspectAuthorizationUtils.canViewSchemaFieldEntity(
                      opContext, SCHEMA_FIELD_URN))
          .thenReturn(false);

      assertFalse(
          EntityAuthorizationUtils.isAPIAuthorizedEntityUrns(
              opContext, READ, List.of(SCHEMA_FIELD_URN)));
    }
  }

  @Test
  public void testIsAPIAuthorizedEntityUrns_schemaFieldReadAllowsWhenRestApiAuthDisabled() {
    authUtilMock.when(AuthUtil::isRestApiAuthorizationEnabled).thenReturn(false);
    try (MockedStatic<EntityAspectAuthorizationUtils> schemaFieldAuth =
        Mockito.mockStatic(EntityAspectAuthorizationUtils.class, Mockito.CALLS_REAL_METHODS)) {
      schemaFieldAuth
          .when(() -> EntityAspectAuthorizationUtils.isSchemaFieldEntity(SCHEMA_FIELD_URN))
          .thenReturn(true);

      assertTrue(
          EntityAuthorizationUtils.isAPIAuthorizedEntityUrns(
              opContext, READ, List.of(SCHEMA_FIELD_URN)));
      schemaFieldAuth.verify(
          () ->
              EntityAspectAuthorizationUtils.canViewSchemaFieldEntity(opContext, SCHEMA_FIELD_URN),
          Mockito.never());
    }
  }

  @Test
  public void testIsAPIAuthorizedEntityUrns_schemaFieldNonReadUsesAuthUtil() {
    authUtilMock
        .when(
            () -> AuthUtil.isAPIAuthorizedEntityUrns(opContext, UPDATE, List.of(SCHEMA_FIELD_URN)))
        .thenReturn(true);
    try (MockedStatic<EntityAspectAuthorizationUtils> schemaFieldAuth =
        Mockito.mockStatic(EntityAspectAuthorizationUtils.class, Mockito.CALLS_REAL_METHODS)) {
      schemaFieldAuth
          .when(() -> EntityAspectAuthorizationUtils.isSchemaFieldEntity(SCHEMA_FIELD_URN))
          .thenReturn(true);

      assertTrue(
          EntityAuthorizationUtils.isAPIAuthorizedEntityUrns(
              opContext, UPDATE, List.of(SCHEMA_FIELD_URN)));
      schemaFieldAuth.verify(
          () ->
              EntityAspectAuthorizationUtils.canViewSchemaFieldEntity(opContext, SCHEMA_FIELD_URN),
          Mockito.never());
      authUtilMock.verify(
          () -> AuthUtil.isAPIAuthorizedEntityUrns(opContext, UPDATE, List.of(SCHEMA_FIELD_URN)));
    }
  }

  @Test
  public void testIsAPIAuthorizedEntityUrns_mixedBatchRoutesSchemaFieldsDocumentsAndOthers() {
    authUtilMock.when(AuthUtil::isRestApiAuthorizationEnabled).thenReturn(true);
    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedEntityUrns(opContext, READ, List.of(DATASET_URN)))
        .thenReturn(true);
    documentAuthMock
        .when(
            () ->
                DocumentAuthorizationUtils.isAPIAuthorizedDocumentUrns(
                    opContext, READ, List.of(DOCUMENT_URN)))
        .thenReturn(true);
    try (MockedStatic<EntityAspectAuthorizationUtils> schemaFieldAuth =
        Mockito.mockStatic(EntityAspectAuthorizationUtils.class, Mockito.CALLS_REAL_METHODS)) {
      schemaFieldAuth
          .when(() -> EntityAspectAuthorizationUtils.isSchemaFieldEntity(SCHEMA_FIELD_URN))
          .thenReturn(true);
      schemaFieldAuth
          .when(() -> EntityAspectAuthorizationUtils.isSchemaFieldEntity(DATASET_URN))
          .thenReturn(false);
      schemaFieldAuth
          .when(() -> EntityAspectAuthorizationUtils.isSchemaFieldEntity(DOCUMENT_URN))
          .thenReturn(false);
      schemaFieldAuth
          .when(
              () ->
                  EntityAspectAuthorizationUtils.canViewSchemaFieldEntity(
                      opContext, SCHEMA_FIELD_URN))
          .thenReturn(true);

      assertTrue(
          EntityAuthorizationUtils.isAPIAuthorizedEntityUrns(
              opContext, READ, List.of(DATASET_URN, SCHEMA_FIELD_URN, DOCUMENT_URN)));

      schemaFieldAuth
          .when(
              () ->
                  EntityAspectAuthorizationUtils.canViewSchemaFieldEntity(
                      opContext, SCHEMA_FIELD_URN))
          .thenReturn(false);
      assertFalse(
          EntityAuthorizationUtils.isAPIAuthorizedEntityUrns(
              opContext, READ, List.of(DATASET_URN, SCHEMA_FIELD_URN, DOCUMENT_URN)));
    }
  }

  @Test
  public void testSearchEntityTypeAuthorizationDefersDocuments() {
    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedEntityType(opContext, READ, List.of("dataset")))
        .thenReturn(true);

    assertTrue(
        EntityAuthorizationUtils.isAPIAuthorizedSearchEntityTypes(opContext, List.of("document")));
    assertTrue(
        EntityAuthorizationUtils.isAPIAuthorizedSearchEntityTypes(
            opContext, List.of("document", "dataset")));
  }

  @Test
  public void testSearchEntityTypeAuthorizationStillGatesNonDocuments() {
    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedEntityType(opContext, READ, List.of("dataset")))
        .thenReturn(false);

    assertFalse(
        EntityAuthorizationUtils.isAPIAuthorizedSearchEntityTypes(
            opContext, List.of("document", "dataset")));
  }

  @Test
  public void testWriteEntityTypeAuthorizationDefersDocuments() {
    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedEntityType(opContext, CREATE, List.of("dataset")))
        .thenReturn(true);

    assertTrue(
        EntityAuthorizationUtils.isAPIAuthorizedWriteEntityTypes(
            opContext, CREATE, List.of("document")));
    assertTrue(
        EntityAuthorizationUtils.isAPIAuthorizedWriteEntityTypes(
            opContext, CREATE, List.of("document", "dataset")));

    authUtilMock.verify(
        () -> AuthUtil.isAPIAuthorizedEntityType(opContext, CREATE, List.of("dataset")));
  }

  @Test
  public void testWriteEntityTypeAuthorizationStillGatesNonDocuments() {
    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedEntityType(opContext, UPDATE, List.of("dataset")))
        .thenReturn(false);

    assertFalse(
        EntityAuthorizationUtils.isAPIAuthorizedWriteEntityTypes(
            opContext, UPDATE, List.of("document", "dataset")));
  }

  @Test
  public void testIngestAuthorizationUsesEffectiveDocumentAuthorizationKey() {
    MetadataChangeProposal proposal =
        new MetadataChangeProposal()
            .setEntityType("document")
            .setEntityUrn(DOCUMENT_URN)
            .setChangeType(ChangeType.UPSERT);
    Pair<ChangeType, Urn> createAuthorizationKey = Pair.of(ChangeType.CREATE_ENTITY, DOCUMENT_URN);
    authUtilMock.when(AuthUtil::isRestApiAuthorizationEnabled).thenReturn(true);
    documentAuthMock
        .when(() -> DocumentAuthorizationUtils.isUpdateLike(ChangeType.UPSERT))
        .thenReturn(true);
    documentAuthMock
        .when(
            () ->
                DocumentAuthorizationUtils.effectiveDocumentIngestAuthorizationKey(
                    ChangeType.UPSERT, DOCUMENT_URN, false))
        .thenReturn(createAuthorizationKey);
    when(aspectRetriever.entityExists(opContext, Set.of(DOCUMENT_URN)))
        .thenReturn(Map.of(DOCUMENT_URN, false));
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIAuthorizedUrns(
                    eq(opContext),
                    eq(ENTITY),
                    eq(Set.of(createAuthorizationKey)),
                    eq(Map.of(DOCUMENT_URN, false))))
        .thenReturn(Map.of(createAuthorizationKey, 403));

    List<Pair<MetadataChangeProposal, Integer>> result =
        EntityAuthorizationUtils.isAPIAuthorizedIngest(
            opContext, mock(EntityRegistry.class), List.of(proposal));

    assertEquals(result, List.of(Pair.of(proposal, 403)));
  }

  @Test
  public void testIngestAuthorization_restApiDisabledDelegatesUnchanged() {
    EntityRegistry entityRegistry = mock(EntityRegistry.class);
    MetadataChangeProposal proposal =
        new MetadataChangeProposal()
            .setEntityType("dataset")
            .setEntityUrn(DATASET_URN)
            .setChangeType(ChangeType.UPSERT);
    List<Pair<MetadataChangeProposal, Integer>> expected = List.of(Pair.of(proposal, 200));
    authUtilMock.when(AuthUtil::isRestApiAuthorizationEnabled).thenReturn(false);
    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorized(opContext, ENTITY, entityRegistry, List.of(proposal)))
        .thenReturn(expected);

    assertEquals(
        EntityAuthorizationUtils.isAPIAuthorizedIngest(
            opContext, entityRegistry, List.of(proposal)),
        expected);
  }

  @Test
  public void testIngestAuthorization_resolvesMissingDocumentUrnFromKeyAspect() {
    EntityRegistry entityRegistry = mock(EntityRegistry.class);
    EntitySpec entitySpec = mock(EntitySpec.class);
    AspectSpec keyAspectSpec = mock(AspectSpec.class);
    when(entityRegistry.getEntitySpec("document")).thenReturn(entitySpec);
    when(entitySpec.getKeyAspectSpec()).thenReturn(keyAspectSpec);
    MetadataChangeProposal proposal =
        new MetadataChangeProposal().setEntityType("document").setChangeType(ChangeType.UPSERT);
    Pair<ChangeType, Urn> createAuthorizationKey = Pair.of(ChangeType.CREATE_ENTITY, DOCUMENT_URN);
    authUtilMock.when(AuthUtil::isRestApiAuthorizationEnabled).thenReturn(true);
    documentAuthMock
        .when(() -> DocumentAuthorizationUtils.isUpdateLike(ChangeType.UPSERT))
        .thenReturn(true);
    documentAuthMock
        .when(
            () ->
                DocumentAuthorizationUtils.effectiveDocumentIngestAuthorizationKey(
                    ChangeType.UPSERT, DOCUMENT_URN, false))
        .thenReturn(createAuthorizationKey);
    when(aspectRetriever.entityExists(opContext, Set.of(DOCUMENT_URN)))
        .thenReturn(Map.of(DOCUMENT_URN, false));
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIAuthorizedUrns(
                    eq(opContext),
                    eq(ENTITY),
                    eq(Set.of(createAuthorizationKey)),
                    eq(Map.of(DOCUMENT_URN, false))))
        .thenReturn(Map.of(createAuthorizationKey, 201));

    try (MockedStatic<EntityKeyUtils> entityKeyUtils = Mockito.mockStatic(EntityKeyUtils.class)) {
      entityKeyUtils
          .when(() -> EntityKeyUtils.getUrnFromProposal(proposal, keyAspectSpec))
          .thenReturn(DOCUMENT_URN);

      assertEquals(
          EntityAuthorizationUtils.isAPIAuthorizedIngest(
              opContext, entityRegistry, List.of(proposal)),
          List.of(Pair.of(proposal, 201)));
    }
  }

  @Test
  public void testIngestAuthorization_nonDocumentUsesExistenceAwareAuth() {
    EntityRegistry entityRegistry = mock(EntityRegistry.class);
    MetadataChangeProposal proposal =
        new MetadataChangeProposal()
            .setEntityType("dataset")
            .setEntityUrn(DATASET_URN)
            .setChangeType(ChangeType.UPSERT);
    Pair<ChangeType, Urn> authorizationKey = Pair.of(ChangeType.UPSERT, DATASET_URN);
    authUtilMock.when(AuthUtil::isRestApiAuthorizationEnabled).thenReturn(true);
    when(aspectRetriever.entityExists(opContext, Set.of(DATASET_URN)))
        .thenReturn(Map.of(DATASET_URN, false));
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIAuthorizedUrns(
                    eq(opContext),
                    eq(ENTITY),
                    eq(Set.of(authorizationKey)),
                    eq(Map.of(DATASET_URN, false))))
        .thenReturn(Map.of(authorizationKey, 200));

    assertEquals(
        EntityAuthorizationUtils.isAPIAuthorizedIngest(
            opContext, entityRegistry, List.of(proposal)),
        List.of(Pair.of(proposal, 200)));
    Mockito.verify(aspectRetriever).entityExists(opContext, Set.of(DATASET_URN));
  }

  @Test
  public void testIsAPIAuthorizedResult_extractsUrnsFromAllResultTypes() {
    documentAuthMock
        .when(
            () ->
                DocumentAuthorizationUtils.isAPIAuthorizedDocumentUrns(
                    eq(opContext), eq(READ), any()))
        .thenReturn(true);

    SearchEntity searchEntity = new SearchEntity().setEntity(DOCUMENT_URN);
    SearchResult searchResult =
        new SearchResult().setEntities(new SearchEntityArray(List.of(searchEntity)));
    ScrollResult scrollResult =
        new ScrollResult().setEntities(new SearchEntityArray(List.of(searchEntity)));
    AutoCompleteResult autoCompleteResult =
        new AutoCompleteResult()
            .setEntities(
                new AutoCompleteEntityArray(
                    List.of(new AutoCompleteEntity().setUrn(DOCUMENT_URN))));
    BrowseResult browseResult =
        new BrowseResult()
            .setEntities(
                new BrowseResultEntityArray(
                    List.of(new BrowseResultEntity().setUrn(DOCUMENT_URN))));

    assertTrue(EntityAuthorizationUtils.isAPIAuthorizedResult(opContext, searchResult));
    assertTrue(EntityAuthorizationUtils.isAPIAuthorizedResult(opContext, scrollResult));
    assertTrue(EntityAuthorizationUtils.isAPIAuthorizedResult(opContext, autoCompleteResult));
    assertTrue(EntityAuthorizationUtils.isAPIAuthorizedResult(opContext, browseResult));

    LineageSearchEntity lineageSearchEntity = new LineageSearchEntity().setEntity(DOCUMENT_URN);
    LineageSearchResult lineageSearchResult =
        new LineageSearchResult()
            .setEntities(new LineageSearchEntityArray(List.of(lineageSearchEntity)));
    LineageScrollResult lineageScrollResult =
        new LineageScrollResult()
            .setEntities(new LineageSearchEntityArray(List.of(lineageSearchEntity)));
    assertTrue(EntityAuthorizationUtils.isAPIAuthorizedResult(opContext, lineageSearchResult));
    assertTrue(EntityAuthorizationUtils.isAPIAuthorizedResult(opContext, lineageScrollResult));
  }

  @Test
  public void testCanViewEntity_delegatesDocuments() {
    documentAuthMock
        .when(() -> DocumentAuthorizationUtils.canViewDocumentEntity(opContext, DOCUMENT_URN))
        .thenReturn(true);
    assertTrue(EntityAuthorizationUtils.canViewEntity(opContext, DOCUMENT_URN));

    authUtilMock.when(() -> AuthUtil.canViewEntity(opContext, DATASET_URN)).thenReturn(false);
    assertFalse(EntityAuthorizationUtils.canViewEntity(opContext, DATASET_URN));
  }

  @Test
  public void testCanViewEntity_delegatesQueries() {
    try (MockedStatic<EntityAspectAuthorizationUtils> queryAuth =
        Mockito.mockStatic(EntityAspectAuthorizationUtils.class)) {
      queryAuth
          .when(
              () ->
                  EntityAspectAuthorizationUtils.canViewQueryEntity(
                      opContext, opContext, aspectRetriever, QUERY_URN))
          .thenReturn(true);

      assertTrue(EntityAuthorizationUtils.canViewEntity(opContext, QUERY_URN));
    }
  }

  @Test
  public void testCanViewEntity_delegatesSchemaFields() {
    try (MockedStatic<EntityAspectAuthorizationUtils> schemaFieldAuth =
        Mockito.mockStatic(EntityAspectAuthorizationUtils.class)) {
      schemaFieldAuth
          .when(
              () ->
                  EntityAspectAuthorizationUtils.canViewSchemaFieldEntity(
                      opContext, SCHEMA_FIELD_URN))
          .thenReturn(true);

      assertTrue(EntityAuthorizationUtils.canViewEntity(opContext, SCHEMA_FIELD_URN));
    }
  }

  @DataProvider(name = "batchItemsAuthorizationCases")
  public Object[][] batchItemsAuthorizationCases() {
    return new Object[][] {
      // Existing entity: CREATE_ENTITY aspect write is existence-aware and can be denied
      {ChangeType.CREATE_ENTITY, "domains", true, 403},
      // Missing entity: UPSERT is authorized as create and allowed
      {ChangeType.UPSERT, "datasetProperties", false, 200},
    };
  }

  @Test(dataProvider = "batchItemsAuthorizationCases")
  public void testBatchItemsAuthorization_existenceAware(
      ChangeType changeType, String aspectName, boolean entityExists, int expectedStatus) {
    BatchItem item = mock(BatchItem.class);
    when(item.getUrn()).thenReturn(DATASET_URN);
    when(item.getChangeType()).thenReturn(changeType);
    when(item.getAspectName()).thenReturn(aspectName);
    Pair<ChangeType, Urn> authorizationKey = Pair.of(changeType, DATASET_URN);
    Map<Urn, Boolean> existence = Map.of(DATASET_URN, entityExists);
    authUtilMock.when(AuthUtil::isRestApiAuthorizationEnabled).thenReturn(true);
    when(aspectRetriever.entityExists(opContext, Set.of(DATASET_URN))).thenReturn(existence);
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIAuthorizedUrns(
                    eq(opContext), eq(ENTITY), eq(Set.of(authorizationKey)), eq(existence)))
        .thenReturn(Map.of(authorizationKey, expectedStatus));

    assertEquals(
        EntityAuthorizationUtils.isAPIAuthorizedBatchItems(opContext, List.of(item)),
        List.of(Pair.of(item, expectedStatus)));
  }
}
