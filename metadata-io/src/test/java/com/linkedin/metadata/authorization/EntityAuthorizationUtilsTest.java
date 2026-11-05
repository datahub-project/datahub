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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.AuthUtil;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataset.DatasetUsageStatistics;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.models.graph.RelatedEntity;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.browse.BrowseResultEntity;
import com.linkedin.metadata.browse.BrowseResultEntityArray;
import com.linkedin.metadata.browse.BrowseResultMetadata;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
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
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.service.DocumentAuthorizationUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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
  private static final Urn ALLOWED_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,allowed,PROD)");
  private static final Urn CHART_URN = UrnUtils.getUrn("urn:li:chart:(looker,facade-chart)");
  private static final Urn DATA_JOB_URN =
      UrnUtils.getUrn("urn:li:dataJob:(urn:li:dataFlow:(airflow,flow,PROD),task)");
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

  private static final Urn RESTRICTED_URN = UrnUtils.getUrn("urn:li:restricted:encrypted");

  /** DATASET_URN is the only entity the actor may not read. */
  private void hideDataset(ApiGroup apiGroup) {
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIAuthorizedEntityUrns(
                    eq(opContext), eq(READ), Mockito.<Collection<Urn>>any()))
        .thenAnswer(
            invocation -> !invocation.<Collection<Urn>>getArgument(2).contains(DATASET_URN));
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIAuthorizedUrns(
                    eq(opContext), eq(apiGroup), eq(READ), Mockito.<Collection<Urn>>any()))
        .thenAnswer(
            invocation -> !invocation.<Collection<Urn>>getArgument(3).contains(DATASET_URN));
  }

  private static SearchEntityArray searchEntities(Urn... urns) {
    return Arrays.stream(urns)
        .map(urn -> new SearchEntity().setEntity(urn))
        .collect(Collectors.toCollection(SearchEntityArray::new));
  }

  private static LineageSearchEntityArray lineageSearchEntities(Urn... urns) {
    return Arrays.stream(urns)
        .map(urn -> new LineageSearchEntity().setEntity(urn))
        .collect(Collectors.toCollection(LineageSearchEntityArray::new));
  }

  @Test
  public void testFilterAPIAuthorizedResult_hiddenMatchAnswersLikeMissing() throws Exception {
    hideDataset(ENTITY);
    SearchResult missing =
        new SearchResult()
            .setEntities(new SearchEntityArray())
            .setNumEntities(0)
            .setFrom(0)
            .setPageSize(10)
            .setMetadata(new SearchResultMetadata());
    SearchResult hidden = missing.copy().setEntities(searchEntities(DATASET_URN)).setNumEntities(1);

    assertEquals(EntityAuthorizationUtils.filterAPIAuthorizedResult(opContext, hidden), missing);
    assertEquals(hidden.getNumEntities(), 1, "the input result must not be modified");

    ScrollResult missingScroll =
        new ScrollResult().setEntities(new SearchEntityArray()).setNumEntities(0).setPageSize(10);
    ScrollResult hiddenScroll =
        missingScroll.copy().setEntities(searchEntities(DATASET_URN)).setNumEntities(1);
    assertEquals(
        EntityAuthorizationUtils.filterAPIAuthorizedResult(opContext, hiddenScroll), missingScroll);

    LineageSearchResult missingLineage =
        new LineageSearchResult()
            .setEntities(new LineageSearchEntityArray())
            .setNumEntities(0)
            .setFrom(0)
            .setPageSize(10)
            .setMetadata(new SearchResultMetadata());
    LineageSearchResult hiddenLineage =
        missingLineage.copy().setEntities(lineageSearchEntities(DATASET_URN)).setNumEntities(1);
    assertEquals(
        EntityAuthorizationUtils.filterAPIAuthorizedResult(opContext, hiddenLineage),
        missingLineage);

    LineageScrollResult missingLineageScroll =
        new LineageScrollResult()
            .setEntities(new LineageSearchEntityArray())
            .setNumEntities(0)
            .setPageSize(10);
    LineageScrollResult hiddenLineageScroll =
        missingLineageScroll
            .copy()
            .setEntities(lineageSearchEntities(DATASET_URN))
            .setNumEntities(1);
    assertEquals(
        EntityAuthorizationUtils.filterAPIAuthorizedResult(opContext, hiddenLineageScroll),
        missingLineageScroll);
  }

  @Test
  public void testFilterAPIAuthorizedResult_keepsReadableEntitiesAndRestrictedPlaceholders() {
    hideDataset(ENTITY);
    SearchResult result =
        new SearchResult()
            .setEntities(searchEntities(ALLOWED_DATASET_URN, DATASET_URN, RESTRICTED_URN))
            .setNumEntities(25);

    SearchResult filtered = EntityAuthorizationUtils.filterAPIAuthorizedResult(opContext, result);

    assertEquals(filtered.getEntities(), searchEntities(ALLOWED_DATASET_URN, RESTRICTED_URN));
    assertEquals(filtered.getNumEntities(), 24);
  }

  @Test
  public void testFilterAPIAuthorizedResult_returnsFullyReadableResultUnchanged() {
    documentAuthMock
        .when(
            () ->
                DocumentAuthorizationUtils.isAPIAuthorizedDocumentUrns(
                    eq(opContext), eq(READ), any()))
        .thenReturn(true);
    SearchResult result =
        new SearchResult().setEntities(searchEntities(DOCUMENT_URN)).setNumEntities(1);

    assertTrue(EntityAuthorizationUtils.filterAPIAuthorizedResult(opContext, result) == result);
  }

  @Test
  public void testFilterAPIAuthorizedResult_dropsHiddenAutoCompleteSuggestions() {
    hideDataset(ENTITY);
    AutoCompleteResult result =
        new AutoCompleteResult()
            .setQuery("fac")
            .setSuggestions(new StringArray(List.of("allowed", "facade")))
            .setEntities(
                new AutoCompleteEntityArray(
                    List.of(
                        new AutoCompleteEntity().setUrn(ALLOWED_DATASET_URN),
                        new AutoCompleteEntity().setUrn(DATASET_URN))));

    AutoCompleteResult filtered =
        EntityAuthorizationUtils.filterAPIAuthorizedResult(opContext, result);

    assertEquals(filtered.getSuggestions(), new StringArray(List.of("allowed")));
    assertEquals(
        filtered.getEntities(),
        new AutoCompleteEntityArray(List.of(new AutoCompleteEntity().setUrn(ALLOWED_DATASET_URN))));
  }

  @Test
  public void testFilterAPIAuthorizedResult_reducesBrowseEntityCounts() {
    hideDataset(ENTITY);
    BrowseResult result =
        new BrowseResult()
            .setEntities(
                new BrowseResultEntityArray(
                    List.of(
                        new BrowseResultEntity().setUrn(ALLOWED_DATASET_URN),
                        new BrowseResultEntity().setUrn(DATASET_URN))))
            .setNumEntities(2)
            .setNumElements(3)
            .setNumGroups(1)
            .setMetadata(new BrowseResultMetadata().setPath("/hive").setTotalNumEntities(5));

    BrowseResult filtered = EntityAuthorizationUtils.filterAPIAuthorizedResult(opContext, result);

    assertEquals(filtered.getEntities().size(), 1);
    assertEquals(filtered.getNumEntities(), 1);
    assertEquals(filtered.getNumElements(), 2);
    assertEquals(filtered.getMetadata().getTotalNumEntities(), 4L);
  }

  @Test
  public void testFilterAPIAuthorizedRelatedEntities_hiddenNeighbourAnswersLikeMissing() {
    hideDataset(ApiGroup.RELATIONSHIP);
    RelatedEntity visible = new RelatedEntity("DownstreamOf", ALLOWED_DATASET_URN.toString());
    RelatedEntity hidden = new RelatedEntity("DownstreamOf", DATASET_URN.toString());

    assertEquals(
        EntityAuthorizationUtils.filterAPIAuthorizedRelatedEntities(
            opContext,
            ApiGroup.RELATIONSHIP,
            new RelatedEntitiesResult(0, 2, 2, List.of(visible, hidden))),
        new RelatedEntitiesResult(0, 1, 1, List.of(visible)));
    assertEquals(
        EntityAuthorizationUtils.filterAPIAuthorizedRelatedEntities(
            opContext, ApiGroup.RELATIONSHIP, new RelatedEntitiesResult(0, 1, 1, List.of(hidden))),
        new RelatedEntitiesResult(0, 0, 0, List.of()));
  }

  @Test
  public void testFilterAPIAuthorizedLineage_hiddenNeighbourAnswersLikeMissing() {
    hideDataset(ApiGroup.LINEAGE);
    LineageRelationship visible =
        new LineageRelationship().setType("DownstreamOf").setEntity(ALLOWED_DATASET_URN);
    LineageRelationship hidden =
        new LineageRelationship().setType("DownstreamOf").setEntity(DATASET_URN);
    EntityLineageResult result =
        new EntityLineageResult()
            .setStart(0)
            .setCount(2)
            .setTotal(2)
            .setRelationships(new LineageRelationshipArray(List.of(visible, hidden)));

    EntityLineageResult filtered =
        EntityAuthorizationUtils.filterAPIAuthorizedLineage(opContext, ApiGroup.LINEAGE, result);

    assertEquals(filtered.getRelationships(), new LineageRelationshipArray(List.of(visible)));
    assertEquals(filtered.getCount(), 1);
    assertEquals(filtered.getTotal(), 1);
    assertEquals(filtered.getFiltered(), 1);
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
  public void testIsAPIAuthorizedEntityUrns_queryReadBypassedForSystemAuth() {
    authUtilMock.when(AuthUtil::isRestApiAuthorizationEnabled).thenReturn(true);
    when(opContext.isSystemAuth()).thenReturn(true);
    try (MockedStatic<EntityAspectAuthorizationUtils> queryAuth =
        Mockito.mockStatic(EntityAspectAuthorizationUtils.class, Mockito.CALLS_REAL_METHODS)) {
      queryAuth
          .when(() -> EntityAspectAuthorizationUtils.isQueryViewAuthorizationEnabled(opContext))
          .thenReturn(true);

      assertTrue(
          EntityAuthorizationUtils.isAPIAuthorizedEntityUrns(opContext, READ, List.of(QUERY_URN)),
          "system-auth query reads must bypass subject-derived filtering, as the GraphQL query"
              + " paths already do");
      queryAuth.verify(
          () ->
              EntityAspectAuthorizationUtils.filterViewableQueryEntities(
                  any(), any(), any(), any(), Mockito.anyBoolean()),
          Mockito.never());
    }
  }

  @Test
  public void testCanViewEntity_delegatesQueries() {
    try (MockedStatic<EntityAspectAuthorizationUtils> queryAuth =
        Mockito.mockStatic(EntityAspectAuthorizationUtils.class)) {
      queryAuth
          .when(
              () ->
                  EntityAspectAuthorizationUtils.canViewQueryEntity(
                      eq(opContext),
                      eq(opContext),
                      eq(aspectRetriever),
                      eq(QUERY_URN),
                      Mockito.anyBoolean()))
          .thenReturn(true);

      assertTrue(EntityAuthorizationUtils.canViewEntity(opContext, QUERY_URN));
    }
  }

  /**
   * canViewEntity's Query-entity branch (search-result masking, related-entity visibility — see
   * ESAccessControlUtil/AuthorizationUtils.canView) shares the same requireAllQuerySubjects
   * resolution as every other Query-entity-subject caller (QueryType, listQueries, REST reads), so
   * that COMPAT's VIEW_AUTHORIZATION_ENABLED-driven single/all switch applies to it uniformly.
   */
  @Test
  public void testCanViewEntity_queriesUseSharedRequireAllSubjects() {
    try (MockedStatic<EntityAspectAuthorizationUtils> queryAuth =
        Mockito.mockStatic(EntityAspectAuthorizationUtils.class)) {
      // Stubbed to false (not true) so a regression that hardcodes true instead of forwarding
      // requireAllQuerySubjects's result fails this test: canViewQueryEntity is only stubbed for
      // eq(false), so a hardcoded-true call site would hit the unstubbed default (false) and
      // assertTrue below would fail.
      queryAuth
          .when(() -> EntityAspectAuthorizationUtils.requireAllQuerySubjects(opContext))
          .thenReturn(false);
      queryAuth
          .when(
              () ->
                  EntityAspectAuthorizationUtils.canViewQueryEntity(
                      eq(opContext), eq(opContext), eq(aspectRetriever), eq(QUERY_URN), eq(false)))
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

  /**
   * Regression coverage for the gap Cursor Bugbot flagged on PR #16319: OpenAPI (v1/v2/v3) and
   * Rest.li entity reads serialize aspect content generically, with no per-field mapper the way
   * GraphQL has, so {@code viewProperties}/{@code dataTransformLogic} were exposed unconditionally
   * on those surfaces even after the GraphQL mappers were gated. {@link
   * EntityAuthorizationUtils#isQuerySqlAspectRestricted} is the shared predicate every one of those
   * surfaces now consults before including either aspect in a response.
   */
  @Test
  public void testIsQuerySqlAspectRestricted_onlyMatchesSqlBearingAspectOnCorrectEntityType() {
    try (MockedStatic<EntityAspectAuthorizationUtils> queryAuth =
        Mockito.mockStatic(EntityAspectAuthorizationUtils.class)) {
      queryAuth
          .when(() -> EntityAspectAuthorizationUtils.canViewQueriesOnEntity(opContext, DATASET_URN))
          .thenReturn(false);
      queryAuth
          .when(
              () -> EntityAspectAuthorizationUtils.canViewQueriesOnEntity(opContext, DATA_JOB_URN))
          .thenReturn(false);
      queryAuth
          .when(() -> EntityAspectAuthorizationUtils.canViewQueriesOnEntity(opContext, CHART_URN))
          .thenReturn(false);

      assertTrue(
          EntityAuthorizationUtils.isQuerySqlAspectRestricted(
              opContext, DATASET_URN, Constants.VIEW_PROPERTIES_ASPECT_NAME),
          "dataset viewProperties must be restricted when the actor lacks query-view access");
      assertTrue(
          EntityAuthorizationUtils.isQuerySqlAspectRestricted(
              opContext, DATA_JOB_URN, Constants.DATA_TRANSFORM_LOGIC_ASPECT_NAME),
          "dataJob dataTransformLogic must be restricted when the actor lacks query-view access");
      assertTrue(
          EntityAuthorizationUtils.isQuerySqlAspectRestricted(
              opContext, CHART_URN, Constants.CHART_QUERY_ASPECT_NAME),
          "chart chartQuery must be restricted when the actor lacks query-view access");
      assertFalse(
          EntityAuthorizationUtils.isQuerySqlAspectRestricted(
              opContext, DATASET_URN, Constants.DATASET_PROPERTIES_ASPECT_NAME),
          "non-SQL-bearing aspects on a dataset are never restricted by this check");
      assertFalse(
          EntityAuthorizationUtils.isQuerySqlAspectRestricted(
              opContext, DATA_JOB_URN, Constants.VIEW_PROPERTIES_ASPECT_NAME),
          "viewProperties is only SQL-bearing on datasets, not data jobs");
      assertFalse(
          EntityAuthorizationUtils.isQuerySqlAspectRestricted(
              opContext, DATASET_URN, Constants.CHART_QUERY_ASPECT_NAME),
          "chartQuery is only SQL-bearing on charts, not datasets");
    }
  }

  @Test
  public void testIsQuerySqlAspectRestricted_falseWhenActorHasQueryViewAccess() {
    try (MockedStatic<EntityAspectAuthorizationUtils> queryAuth =
        Mockito.mockStatic(EntityAspectAuthorizationUtils.class)) {
      queryAuth
          .when(() -> EntityAspectAuthorizationUtils.canViewQueriesOnEntity(opContext, DATASET_URN))
          .thenReturn(true);

      assertFalse(
          EntityAuthorizationUtils.isQuerySqlAspectRestricted(
              opContext, DATASET_URN, Constants.VIEW_PROPERTIES_ASPECT_NAME));
    }
  }

  /**
   * Regression coverage for the confirmed disclosure path where generic timeseries-aspect reads
   * (Rest.li {@code AspectResource#getTimeseriesAspectValues}, OpenAPI v2 {@code
   * TimeseriesController}, v3 {@code EntityController}'s timeseries branch, GraphQL's raw-aspect
   * resolver) returned {@code topSqlQueries} unconditionally, since none of them consulted the
   * escape hatch GraphQL's {@code DatasetUsageStatsResolver} already had. Unlike {@code
   * viewProperties}/{@code dataTransformLogic}/{@code chartQuery}, only this one field is withheld
   * — the numeric usage counts alongside it must survive.
   */
  @Test
  public void testStripTopSqlQueriesFromRawAspect_removesFieldOnlyWhenRestricted() {
    try (MockedStatic<EntityAspectAuthorizationUtils> queryAuth =
        Mockito.mockStatic(EntityAspectAuthorizationUtils.class)) {
      queryAuth
          .when(
              () ->
                  EntityAspectAuthorizationUtils.isTopSqlQueriesRestricted(opContext, DATASET_URN))
          .thenReturn(true);

      DataMap aspectValue = new DataMap();
      aspectValue.put("topSqlQueries", new StringArray("select secret").data());
      aspectValue.put("totalSqlQueries", 5);

      EntityAuthorizationUtils.stripTopSqlQueriesFromRawAspect(
          opContext, DATASET_URN, Constants.DATASET_USAGE_STATISTICS_ASPECT_NAME, aspectValue);

      assertFalse(
          aspectValue.containsKey("topSqlQueries"),
          "topSqlQueries must be stripped when restricted");
      assertTrue(
          aspectValue.containsKey("totalSqlQueries"), "numeric usage stats must remain intact");
    }
  }

  @Test
  public void testStripTopSqlQueriesFromRawAspect_keepsFieldWhenActorHasAccess() {
    try (MockedStatic<EntityAspectAuthorizationUtils> queryAuth =
        Mockito.mockStatic(EntityAspectAuthorizationUtils.class)) {
      queryAuth
          .when(
              () ->
                  EntityAspectAuthorizationUtils.isTopSqlQueriesRestricted(opContext, DATASET_URN))
          .thenReturn(false);

      DataMap aspectValue = new DataMap();
      aspectValue.put("topSqlQueries", new StringArray("select allowed").data());

      EntityAuthorizationUtils.stripTopSqlQueriesFromRawAspect(
          opContext, DATASET_URN, Constants.DATASET_USAGE_STATISTICS_ASPECT_NAME, aspectValue);

      assertTrue(aspectValue.containsKey("topSqlQueries"));
    }
  }

  @Test
  public void testStripTopSqlQueriesFromRawAspect_noopForNonUsageStatisticsAspect() {
    DataMap aspectValue = new DataMap();
    aspectValue.put("topSqlQueries", new StringArray("select secret").data());

    EntityAuthorizationUtils.stripTopSqlQueriesFromRawAspect(
        opContext, DATASET_URN, Constants.VIEW_PROPERTIES_ASPECT_NAME, aspectValue);

    assertTrue(
        aspectValue.containsKey("topSqlQueries"),
        "only datasetUsageStatistics is ever subject to this check");
  }

  /**
   * Same coverage as {@link #testStripTopSqlQueriesFromRawAspect_removesFieldOnlyWhenRestricted},
   * but for the {@code GenericAspect}-serialized-bytes shape Rest.li's {@code
   * getTimeseriesAspectValues} and OpenAPI v2's {@code TimeseriesController} carry timeseries
   * values in, rather than a live DataMap.
   */
  @Test
  public void testStripTopSqlQueriesFromEnvelopedAspect_stripsFieldWhenRestricted() {
    try (MockedStatic<EntityAspectAuthorizationUtils> queryAuth =
        Mockito.mockStatic(EntityAspectAuthorizationUtils.class)) {
      queryAuth
          .when(
              () ->
                  EntityAspectAuthorizationUtils.isTopSqlQueriesRestricted(opContext, DATASET_URN))
          .thenReturn(true);

      DatasetUsageStatistics stats =
          new DatasetUsageStatistics()
              .setTimestampMillis(0L)
              .setTopSqlQueries(new StringArray("select secret"))
              .setTotalSqlQueries(1);
      com.linkedin.metadata.aspect.EnvelopedAspect envelopedAspect =
          new com.linkedin.metadata.aspect.EnvelopedAspect()
              .setAspect(GenericRecordUtils.serializeAspect(stats));

      EntityAuthorizationUtils.stripTopSqlQueriesFromEnvelopedAspect(
          opContext, DATASET_URN, Constants.DATASET_USAGE_STATISTICS_ASPECT_NAME, envelopedAspect);

      DatasetUsageStatistics result =
          GenericRecordUtils.deserializeAspect(
              envelopedAspect.getAspect().getValue(),
              envelopedAspect.getAspect().getContentType(),
              DatasetUsageStatistics.class);
      assertNull(result.getTopSqlQueries(), "topSqlQueries must be stripped when restricted");
      assertEquals(
          (int) result.getTotalSqlQueries(), 1, "numeric usage stats must survive the round trip");
    }
  }

  @Test
  public void testCompletelyRedactUnauthorizedQuerySqlAspects_removesOnlyRestrictedAspects() {
    try (MockedStatic<EntityAspectAuthorizationUtils> queryAuth =
        Mockito.mockStatic(EntityAspectAuthorizationUtils.class)) {
      queryAuth
          .when(() -> EntityAspectAuthorizationUtils.canViewQueriesOnEntity(opContext, DATASET_URN))
          .thenReturn(false);
      queryAuth
          .when(
              () ->
                  EntityAspectAuthorizationUtils.canViewQueriesOnEntity(
                      opContext, ALLOWED_DATASET_URN))
          .thenReturn(true);

      EntityResponse restrictedDataset = entityResponseWithViewProperties(DATASET_URN);
      EntityResponse allowedDataset = entityResponseWithViewProperties(ALLOWED_DATASET_URN);
      Map<Urn, EntityResponse> responses =
          Map.of(DATASET_URN, restrictedDataset, ALLOWED_DATASET_URN, allowedDataset);

      EntityAuthorizationUtils.completelyRedactUnauthorizedQuerySqlAspects(opContext, responses);

      assertFalse(
          restrictedDataset.getAspects().containsKey(Constants.VIEW_PROPERTIES_ASPECT_NAME),
          "viewProperties must be removed for an actor lacking query-view access");
      assertTrue(
          allowedDataset.getAspects().containsKey(Constants.VIEW_PROPERTIES_ASPECT_NAME),
          "viewProperties must remain for an actor with query-view access");
    }
  }

  private static EntityResponse entityResponseWithViewProperties(Urn urn) {
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.VIEW_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect()
            .setName(Constants.VIEW_PROPERTIES_ASPECT_NAME)
            .setValue(new Aspect()));
    return new EntityResponse()
        .setUrn(urn)
        .setEntityName(Constants.DATASET_ENTITY_NAME)
        .setAspects(aspects);
  }
}
