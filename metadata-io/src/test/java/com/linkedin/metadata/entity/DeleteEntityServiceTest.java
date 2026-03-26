package com.linkedin.metadata.entity;

import static com.linkedin.metadata.search.utils.QueryUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.FormAssociation;
import com.linkedin.common.FormAssociationArray;
import com.linkedin.common.FormVerificationAssociationArray;
import com.linkedin.common.Forms;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.container.Container;
import com.linkedin.entity.EntityResponse;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.file.BucketStorageLocation;
import com.linkedin.file.DataHubFileInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.run.DeleteReferencesResponse;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.metadata.utils.aws.S3Util;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DeleteEntityServiceTest {

  protected OperationContext opContext;
  protected EbeanAspectDao _aspectDao;
  protected EntityServiceImpl _entityServiceImpl;
  protected GraphService _graphService = Mockito.mock(GraphService.class);
  protected DeleteEntityService _deleteEntityService;
  protected UpdateIndicesService _mockUpdateIndicesService;
  protected EntitySearchService _mockSearchService;

  private static RelatedEntitiesScrollResult emptyScrollGraphResult() {
    return RelatedEntitiesScrollResult.builder()
        .numResults(0)
        .pageSize(0)
        .scrollId(null)
        .entities(ImmutableList.of())
        .build();
  }

  private static RelatedEntitiesScrollResult scrollGraphResult(
      int numResults, String scrollId, List<RelatedEntities> entities) {
    return RelatedEntitiesScrollResult.builder()
        .numResults(numResults)
        .pageSize(entities.size())
        .scrollId(scrollId)
        .entities(entities)
        .build();
  }

  /** Creates a RelatedEntities for INCOMING direction (entity references the deleted URN). */
  private static RelatedEntities incomingRelation(
      String relationshipType, String entityUrn, String deletedUrn) {
    return new RelatedEntities(
        relationshipType, entityUrn, deletedUrn, RelationshipDirection.INCOMING, null);
  }

  public DeleteEntityServiceTest() {
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    _aspectDao = mock(EbeanAspectDao.class);
    _mockUpdateIndicesService = mock(UpdateIndicesService.class);
    _mockSearchService = mock(EntitySearchService.class);
    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    _entityServiceImpl =
        new EntityServiceImpl(_aspectDao, mock(EventProducer.class), true, preProcessHooks, true);
    _entityServiceImpl.setUpdateIndicesService(_mockUpdateIndicesService);
    _deleteEntityService =
        new DeleteEntityService(_entityServiceImpl, _graphService, _mockSearchService, null);

    setupDefaultFileScrollMock(_mockSearchService);
  }

  /**
   * Setup default mock for file searches to return empty results. This is needed because
   * deleteFileReferences is called for every entity deletion.
   */
  private void setupDefaultFileScrollMock(EntitySearchService searchService) {
    ScrollResult emptyFileScrollResult = new ScrollResult();
    emptyFileScrollResult.setEntities(new SearchEntityArray());
    emptyFileScrollResult.setNumEntities(0);

    // Mock file entity searches to return empty results
    // Use lenient() so other, more specific mocks can override this
    Mockito.lenient()
        .when(
            searchService.structuredScroll(
                Mockito.any(OperationContext.class),
                Mockito.argThat(
                    set -> set != null && set.contains(Constants.DATAHUB_FILE_ENTITY_NAME)),
                Mockito.eq("*"),
                Mockito.any(Filter.class),
                Mockito.isNull(),
                Mockito.any(),
                Mockito.eq("5m"),
                Mockito.anyInt()))
        .thenReturn(emptyFileScrollResult);
  }

  /**
   * This test checks whether deleting non array references in PDL aspects generates a valid MCP.
   */
  @Test
  public void testDeleteUniqueRefGeneratesValidMCP() {
    // Explicitly set up file mock to return empty results (no files to delete)
    ScrollResult emptyFileScrollResult = new ScrollResult();
    emptyFileScrollResult.setEntities(new SearchEntityArray());
    emptyFileScrollResult.setNumEntities(0);
    Mockito.when(
            _mockSearchService.structuredScroll(
                Mockito.any(OperationContext.class),
                Mockito.argThat(
                    set -> set != null && set.contains(Constants.DATAHUB_FILE_ENTITY_NAME)),
                Mockito.eq("*"),
                Mockito.any(Filter.class),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq("5m"),
                Mockito.anyInt()))
        .thenReturn(emptyFileScrollResult);

    final Urn dataset = UrnUtils.toDatasetUrn("snowflake", "test", "DEV");
    final Urn container = UrnUtils.getUrn("urn:li:container:d1006cf3-3ff9-48e3-85cd-26eb23775ab2");

    final RelatedEntitiesScrollResult mockScrollResult =
        scrollGraphResult(
            1,
            null,
            ImmutableList.of(
                incomingRelation("IsPartOf", dataset.toString(), container.toString())));

    Mockito.when(
            _graphService.scrollRelatedEntities(
                any(OperationContext.class),
                nullable(Set.class),
                eq(newFilter("urn", container.toString())),
                nullable(Set.class),
                eq(EMPTY_FILTER),
                eq(ImmutableSet.of()),
                eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
                eq(Edge.EDGE_SORT_CRITERION),
                nullable(String.class),
                eq("5m"),
                eq(1000),
                nullable(Long.class),
                nullable(Long.class)))
        .thenReturn(mockScrollResult);

    final EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(dataset);
    entityResponse.setEntityName(dataset.getEntityType());
    final Container containerAspect = new Container();
    containerAspect.setContainer(container);
    final EntityAspectIdentifier dbKey =
        new EntityAspectIdentifier(dataset.toString(), Constants.CONTAINER_ASPECT_NAME, 0);

    final EntityAspect dbValue = new EntityAspect();
    dbValue.setUrn(dataset.toString());
    dbValue.setVersion(0);
    dbValue.setAspect(Constants.CONTAINER_ASPECT_NAME);
    dbValue.setMetadata(RecordUtils.toJsonString(containerAspect));
    dbValue.setSystemMetadata(
        RecordUtils.toJsonString(SystemMetadataUtils.createDefaultSystemMetadata()));
    final AuditStamp auditStamp = AuditStampUtils.createDefaultAuditStamp();
    dbValue.setCreatedBy(auditStamp.getActor().toString());
    dbValue.setCreatedOn(new Timestamp(auditStamp.getTime()));

    final Map<EntityAspectIdentifier, EntityAspect> dbEntries = Map.of(dbKey, dbValue);
    Mockito.when(_aspectDao.batchGet(Mockito.any(), Mockito.anyBoolean())).thenReturn(dbEntries);

    RollbackResult result =
        new RollbackResult(
            dataset,
            Constants.DATASET_ENTITY_NAME,
            Constants.CONTAINER_ASPECT_NAME,
            containerAspect,
            null,
            null,
            null,
            ChangeType.DELETE,
            false,
            1);

    Mockito.when(_aspectDao.runInTransactionWithRetry(Mockito.any(), Mockito.anyInt()))
        .thenReturn(Optional.of(result));

    final DeleteReferencesResponse response =
        _deleteEntityService.deleteReferencesTo(opContext, container, false);
    assertEquals(1, (int) response.getTotal());
    assertFalse(response.getRelatedAspects().isEmpty());
  }

  /** This test checks whether updating search references works properly (for forms only for now) */
  @Test
  public void testDeleteSearchReferences() {
    EntityService<?> mockEntityService = Mockito.mock(EntityService.class);
    EntitySearchService mockSearchService = Mockito.mock(EntitySearchService.class);
    DeleteEntityService deleteEntityService =
        new DeleteEntityService(mockEntityService, _graphService, mockSearchService, null);

    final Urn dataset = UrnUtils.toDatasetUrn("snowflake", "test", "DEV");
    final Urn form = UrnUtils.getUrn("urn:li:form:12345");

    // Mock file entity searches to return empty results (no files to delete)
    ScrollResult emptyFileScrollResult = new ScrollResult();
    emptyFileScrollResult.setEntities(new SearchEntityArray());
    emptyFileScrollResult.setNumEntities(0);
    Mockito.when(
            mockSearchService.structuredScroll(
                Mockito.any(OperationContext.class),
                Mockito.argThat(
                    set -> set != null && set.contains(Constants.DATAHUB_FILE_ENTITY_NAME)),
                Mockito.eq("*"),
                Mockito.any(Filter.class),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq("5m"),
                Mockito.anyInt()))
        .thenReturn(emptyFileScrollResult);

    ScrollResult scrollResult = new ScrollResult();
    SearchEntityArray entities = new SearchEntityArray();
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(dataset);
    entities.add(searchEntity);
    scrollResult.setEntities(entities);
    scrollResult.setNumEntities(1);
    scrollResult.setScrollId("1");
    Mockito.when(
            mockSearchService.structuredScroll(
                Mockito.any(OperationContext.class),
                Mockito.argThat(
                    set -> set == null || !set.contains(Constants.DATAHUB_FILE_ENTITY_NAME)),
                Mockito.eq("*"),
                Mockito.any(Filter.class),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq("5m"),
                Mockito.eq(1000)))
        .thenReturn(scrollResult);

    ScrollResult scrollResult2 = new ScrollResult();
    scrollResult2.setNumEntities(0);
    Mockito.when(
            mockSearchService.structuredScroll(
                Mockito.any(OperationContext.class),
                Mockito.argThat(
                    set -> set == null || !set.contains(Constants.DATAHUB_FILE_ENTITY_NAME)),
                Mockito.eq("*"),
                Mockito.any(Filter.class),
                Mockito.eq(null),
                Mockito.eq("1"),
                Mockito.eq("5m"),
                Mockito.eq(1000)))
        .thenReturn(scrollResult2);

    Forms formsAspect = new Forms();
    FormAssociationArray incompleteForms = new FormAssociationArray();
    FormAssociation formAssociation = new FormAssociation();
    formAssociation.setUrn(form);
    incompleteForms.add(formAssociation);
    formsAspect.setIncompleteForms(incompleteForms);
    formsAspect.setCompletedForms(new FormAssociationArray());
    formsAspect.setVerifications(new FormVerificationAssociationArray());
    Mockito.when(
            mockEntityService.getLatestAspect(
                Mockito.any(OperationContext.class), Mockito.eq(dataset), Mockito.eq("forms")))
        .thenReturn(formsAspect);

    // no entities with relationships on forms
    Mockito.when(
            _graphService.scrollRelatedEntities(
                any(OperationContext.class),
                nullable(Set.class),
                eq(newFilter("urn", form.toString())),
                nullable(Set.class),
                eq(EMPTY_FILTER),
                eq(ImmutableSet.of()),
                eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
                eq(Edge.EDGE_SORT_CRITERION),
                nullable(String.class),
                eq("5m"),
                eq(1000),
                nullable(Long.class),
                nullable(Long.class)))
        .thenReturn(emptyScrollGraphResult());

    final DeleteReferencesResponse response =
        deleteEntityService.deleteReferencesTo(opContext, form, false);

    // ensure we ingest one MCP for cleaning up forms reference
    Mockito.verify(mockEntityService, Mockito.times(1))
        .ingestProposal(
            any(),
            Mockito.any(MetadataChangeProposal.class),
            Mockito.any(AuditStamp.class),
            Mockito.eq(true));
    assertEquals(1, (int) response.getTotal());
    assertTrue(response.getRelatedAspects().isEmpty());
  }

  /** This test ensures we aren't issuing MCPs if there are no search references */
  @Test
  public void testDeleteNoSearchReferences() {
    EntityService<?> mockEntityService = Mockito.mock(EntityService.class);
    EntitySearchService mockSearchService = Mockito.mock(EntitySearchService.class);
    DeleteEntityService deleteEntityService =
        new DeleteEntityService(mockEntityService, _graphService, mockSearchService, null);

    final Urn dataset = UrnUtils.toDatasetUrn("snowflake", "test", "DEV");
    final Urn form = UrnUtils.getUrn("urn:li:form:12345");

    // Mock file entity searches to return empty results (no files to delete)
    ScrollResult emptyFileScrollResult = new ScrollResult();
    emptyFileScrollResult.setEntities(new SearchEntityArray());
    emptyFileScrollResult.setNumEntities(0);
    Mockito.when(
            mockSearchService.structuredScroll(
                Mockito.any(OperationContext.class),
                Mockito.argThat(
                    set -> set != null && set.contains(Constants.DATAHUB_FILE_ENTITY_NAME)),
                Mockito.eq("*"),
                Mockito.any(Filter.class),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq("5m"),
                Mockito.anyInt()))
        .thenReturn(emptyFileScrollResult);

    ScrollResult scrollResult = new ScrollResult();
    scrollResult.setEntities(new SearchEntityArray());
    scrollResult.setNumEntities(0);
    Mockito.when(
            mockSearchService.structuredScroll(
                Mockito.any(OperationContext.class),
                Mockito.argThat(
                    set -> set == null || !set.contains(Constants.DATAHUB_FILE_ENTITY_NAME)),
                Mockito.eq("*"),
                Mockito.any(Filter.class),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq("5m"),
                Mockito.eq(1000)))
        .thenReturn(scrollResult);

    // no entities with relationships on forms
    Mockito.when(
            _graphService.scrollRelatedEntities(
                any(OperationContext.class),
                nullable(Set.class),
                eq(newFilter("urn", form.toString())),
                nullable(Set.class),
                eq(EMPTY_FILTER),
                eq(ImmutableSet.of()),
                eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
                eq(Edge.EDGE_SORT_CRITERION),
                nullable(String.class),
                eq("5m"),
                eq(1000),
                nullable(Long.class),
                nullable(Long.class)))
        .thenReturn(emptyScrollGraphResult());

    final DeleteReferencesResponse response =
        deleteEntityService.deleteReferencesTo(opContext, form, false);

    // ensure we did not ingest anything if there are no references
    Mockito.verify(mockEntityService, Mockito.times(0))
        .ingestProposal(
            any(),
            Mockito.any(MetadataChangeProposal.class),
            Mockito.any(AuditStamp.class),
            Mockito.eq(true));
    assertEquals(0, (int) response.getTotal());
    assertTrue(response.getRelatedAspects().isEmpty());
  }

  /** This test checks to make sure we don't issue MCPs if this is a dry-run */
  @Test
  public void testDeleteSearchReferencesDryRun() {
    EntityService<?> mockEntityService = Mockito.mock(EntityService.class);
    EntitySearchService mockSearchService = Mockito.mock(EntitySearchService.class);
    DeleteEntityService deleteEntityService =
        new DeleteEntityService(mockEntityService, _graphService, mockSearchService, null);

    final Urn dataset = UrnUtils.toDatasetUrn("snowflake", "test", "DEV");
    final Urn form = UrnUtils.getUrn("urn:li:form:12345");

    // Mock file entity searches to return empty results (no files to delete)
    ScrollResult emptyFileScrollResult = new ScrollResult();
    emptyFileScrollResult.setEntities(new SearchEntityArray());
    emptyFileScrollResult.setNumEntities(0);
    Mockito.when(
            mockSearchService.structuredScroll(
                Mockito.any(OperationContext.class),
                Mockito.argThat(
                    set -> set != null && set.contains(Constants.DATAHUB_FILE_ENTITY_NAME)),
                Mockito.eq("*"),
                Mockito.any(Filter.class),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq("5m"),
                Mockito.anyInt()))
        .thenReturn(emptyFileScrollResult);

    ScrollResult scrollResult = new ScrollResult();
    SearchEntityArray entities = new SearchEntityArray();
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(dataset);
    entities.add(searchEntity);
    scrollResult.setEntities(entities);
    scrollResult.setNumEntities(1);
    scrollResult.setScrollId("1");
    Mockito.when(
            mockSearchService.structuredScroll(
                Mockito.any(OperationContext.class),
                Mockito.argThat(
                    set -> set == null || !set.contains(Constants.DATAHUB_FILE_ENTITY_NAME)),
                Mockito.eq("*"),
                Mockito.any(Filter.class),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq("5m"),
                Mockito.eq(1000)))
        .thenReturn(scrollResult);

    ScrollResult scrollResult2 = new ScrollResult();
    scrollResult2.setNumEntities(0);
    Mockito.when(
            mockSearchService.structuredScroll(
                Mockito.any(OperationContext.class),
                Mockito.argThat(
                    set -> set == null || !set.contains(Constants.DATAHUB_FILE_ENTITY_NAME)),
                Mockito.eq("*"),
                Mockito.any(Filter.class),
                Mockito.eq(null),
                Mockito.eq("1"),
                Mockito.eq("5m"),
                Mockito.eq(1000)))
        .thenReturn(scrollResult2);

    // no entities with relationships on forms
    Mockito.when(
            _graphService.scrollRelatedEntities(
                any(OperationContext.class),
                nullable(Set.class),
                eq(newFilter("urn", form.toString())),
                nullable(Set.class),
                eq(EMPTY_FILTER),
                eq(ImmutableSet.of()),
                eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
                eq(Edge.EDGE_SORT_CRITERION),
                nullable(String.class),
                eq("5m"),
                eq(1000),
                nullable(Long.class),
                nullable(Long.class)))
        .thenReturn(emptyScrollGraphResult());

    final DeleteReferencesResponse response =
        deleteEntityService.deleteReferencesTo(opContext, form, false);

    // ensure we do not ingest anything since this is dry-run, but the total returns 1
    Mockito.verify(mockEntityService, Mockito.times(0))
        .ingestProposal(
            any(),
            Mockito.any(MetadataChangeProposal.class),
            Mockito.any(AuditStamp.class),
            Mockito.eq(true));
    assertEquals(1, (int) response.getTotal());
    assertTrue(response.getRelatedAspects().isEmpty());
  }

  /**
   * Tests that dry-run with graph references returns the correct total and does not process any
   * deletions. The scroll-based pagination should not continue past the first page in dry-run mode.
   */
  @Test
  public void testDryRunWithGraphReferencesReturnsCountWithoutProcessing()
      throws URISyntaxException {
    EntityService<?> mockEntityService = Mockito.mock(EntityService.class);
    EntitySearchService mockSearchService = Mockito.mock(EntitySearchService.class);
    GraphService mockGraphService = Mockito.mock(GraphService.class);
    DeleteEntityService deleteEntityService =
        new DeleteEntityService(mockEntityService, mockGraphService, mockSearchService, null);

    final Urn container = UrnUtils.getUrn("urn:li:container:dry-run-test");
    final String containerStr = container.toString();

    // Graph has 3 referencing entities across 2 pages — but dry-run should not paginate
    ImmutableList<RelatedEntities> entities =
        ImmutableList.of(
            incomingRelation(
                "IsPartOf", "urn:li:dataset:(urn:li:dataPlatform:s,t1,PROD)", containerStr),
            incomingRelation(
                "IsPartOf", "urn:li:dataset:(urn:li:dataPlatform:s,t2,PROD)", containerStr));

    RelatedEntitiesScrollResult firstPage = scrollGraphResult(3, "scroll-1", entities);

    Mockito.when(
            mockGraphService.scrollRelatedEntities(
                any(OperationContext.class),
                nullable(Set.class),
                eq(newFilter("urn", containerStr)),
                nullable(Set.class),
                eq(EMPTY_FILTER),
                eq(ImmutableSet.of()),
                eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
                eq(Edge.EDGE_SORT_CRITERION),
                nullable(String.class),
                eq("5m"),
                eq(1000),
                nullable(Long.class),
                nullable(Long.class)))
        .thenReturn(firstPage);

    // Mock empty search results (files, forms, etc.)
    ScrollResult emptyScrollResult = new ScrollResult();
    emptyScrollResult.setEntities(new SearchEntityArray());
    emptyScrollResult.setNumEntities(0);
    Mockito.when(
            mockSearchService.structuredScroll(
                Mockito.any(OperationContext.class),
                Mockito.any(),
                Mockito.eq("*"),
                Mockito.any(Filter.class),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.anyInt()))
        .thenReturn(emptyScrollResult);

    // Mock entity lookups for the preview (getRelatedAspectStream)
    Mockito.when(
            mockEntityService.getEntityV2(
                any(OperationContext.class),
                Mockito.anyString(),
                Mockito.any(Urn.class),
                Mockito.anySet()))
        .thenReturn(null);

    final DeleteReferencesResponse response =
        deleteEntityService.deleteReferencesTo(opContext, container, true);

    // Total should reflect graph count (3) even though only 2 were in the first page
    assertEquals(3, (int) response.getTotal());

    // Dry-run: scrollRelatedEntities should be called exactly once (no pagination)
    Mockito.verify(mockGraphService, Mockito.times(1))
        .scrollRelatedEntities(
            any(OperationContext.class),
            nullable(Set.class),
            eq(newFilter("urn", containerStr)),
            nullable(Set.class),
            eq(EMPTY_FILTER),
            eq(ImmutableSet.of()),
            eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
            eq(Edge.EDGE_SORT_CRITERION),
            nullable(String.class),
            eq("5m"),
            eq(1000),
            nullable(Long.class),
            nullable(Long.class));

    // No deletions should have been attempted
    Mockito.verify(mockEntityService, Mockito.never())
        .ingestProposal(
            any(),
            Mockito.any(MetadataChangeProposal.class),
            Mockito.any(AuditStamp.class),
            Mockito.anyBoolean());
  }

  /** Test that file cleanup is triggered when deleting an entity with files */
  @Test
  public void testDeleteFileReferences() {
    EntityService<?> mockEntityService = Mockito.mock(EntityService.class);
    S3Util mockS3Util = Mockito.mock(S3Util.class);
    DeleteEntityService deleteEntityService =
        new DeleteEntityService(mockEntityService, _graphService, _mockSearchService, mockS3Util);

    final Urn dataset = UrnUtils.toDatasetUrn("snowflake", "test", "DEV");
    final Urn fileUrn = UrnUtils.getUrn("urn:li:dataHubFile:test-file-id");

    // Mock file search result
    ScrollResult fileScrollResult = new ScrollResult();
    SearchEntityArray fileEntities = new SearchEntityArray();
    SearchEntity fileEntity = new SearchEntity();
    fileEntity.setEntity(fileUrn);
    fileEntities.add(fileEntity);
    fileScrollResult.setEntities(fileEntities);
    fileScrollResult.setNumEntities(1);
    fileScrollResult.setScrollId("1");

    Mockito.when(
            _mockSearchService.structuredScroll(
                Mockito.any(OperationContext.class),
                Mockito.argThat(
                    set ->
                        set != null
                            && set.size() == 1
                            && set.contains(Constants.DATAHUB_FILE_ENTITY_NAME)),
                Mockito.eq("*"),
                Mockito.any(Filter.class),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq("5m"),
                Mockito.eq(1000)))
        .thenReturn(fileScrollResult);

    ScrollResult emptyScrollResult = new ScrollResult();
    emptyScrollResult.setNumEntities(0);
    Mockito.when(
            _mockSearchService.structuredScroll(
                Mockito.any(OperationContext.class),
                Mockito.argThat(
                    set ->
                        set != null
                            && set.size() == 1
                            && set.contains(Constants.DATAHUB_FILE_ENTITY_NAME)),
                Mockito.eq("*"),
                Mockito.any(Filter.class),
                Mockito.eq(null),
                Mockito.eq("1"),
                Mockito.eq("5m"),
                Mockito.eq(1000)))
        .thenReturn(emptyScrollResult);

    // Mock file info aspect
    DataHubFileInfo fileInfo = new DataHubFileInfo();
    fileInfo.setReferencedByAsset(dataset);
    BucketStorageLocation location = new BucketStorageLocation();
    location.setStorageBucket("test-bucket");
    location.setStorageKey("test-key");
    fileInfo.setBucketStorageLocation(location);

    Mockito.when(
            mockEntityService.getLatestAspect(
                Mockito.any(OperationContext.class),
                eq(fileUrn),
                eq(Constants.DATAHUB_FILE_INFO_ASPECT_NAME)))
        .thenReturn(fileInfo);

    // No other relationships
    Mockito.when(
            _graphService.scrollRelatedEntities(
                any(OperationContext.class),
                nullable(Set.class),
                eq(newFilter("urn", dataset.toString())),
                nullable(Set.class),
                eq(EMPTY_FILTER),
                eq(ImmutableSet.of()),
                eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
                eq(Edge.EDGE_SORT_CRITERION),
                nullable(String.class),
                eq("5m"),
                eq(1000),
                nullable(Long.class),
                nullable(Long.class)))
        .thenReturn(emptyScrollGraphResult());

    final DeleteReferencesResponse response =
        deleteEntityService.deleteReferencesTo(opContext, dataset, false);

    // Verify S3 delete was called
    Mockito.verify(mockS3Util, Mockito.times(1)).deleteObject("test-bucket", "test-key");

    // Verify file entity was soft-deleted
    Mockito.verify(mockEntityService, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.any(MetadataChangeProposal.class),
            Mockito.any(AuditStamp.class),
            eq(true));
  }

  /** Test that file cleanup is skipped when S3Util is not configured */
  @Test
  public void testDeleteFileReferencesWithoutS3Util() {
    EntityService<?> mockEntityService = Mockito.mock(EntityService.class);
    DeleteEntityService deleteEntityService =
        new DeleteEntityService(mockEntityService, _graphService, _mockSearchService, null);

    final Urn dataset = UrnUtils.toDatasetUrn("snowflake", "test", "DEV");
    final Urn fileUrn = UrnUtils.getUrn("urn:li:dataHubFile:test-file-id");

    // Mock file search result
    ScrollResult fileScrollResult = new ScrollResult();
    SearchEntityArray fileEntities = new SearchEntityArray();
    SearchEntity fileEntity = new SearchEntity();
    fileEntity.setEntity(fileUrn);
    fileEntities.add(fileEntity);
    fileScrollResult.setEntities(fileEntities);
    fileScrollResult.setNumEntities(1);
    fileScrollResult.setScrollId("1");

    Mockito.when(
            _mockSearchService.structuredScroll(
                Mockito.any(OperationContext.class),
                Mockito.argThat(
                    set ->
                        set != null
                            && set.size() == 1
                            && set.contains(Constants.DATAHUB_FILE_ENTITY_NAME)),
                Mockito.eq("*"),
                Mockito.any(Filter.class),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq("5m"),
                Mockito.eq(1000)))
        .thenReturn(fileScrollResult);

    ScrollResult emptyScrollResult = new ScrollResult();
    emptyScrollResult.setNumEntities(0);
    Mockito.when(
            _mockSearchService.structuredScroll(
                Mockito.any(OperationContext.class),
                Mockito.argThat(
                    set ->
                        set != null
                            && set.size() == 1
                            && set.contains(Constants.DATAHUB_FILE_ENTITY_NAME)),
                Mockito.eq("*"),
                Mockito.any(Filter.class),
                Mockito.eq(null),
                Mockito.eq("1"),
                Mockito.eq("5m"),
                Mockito.eq(1000)))
        .thenReturn(emptyScrollResult);

    // Mock file info aspect
    DataHubFileInfo fileInfo = new DataHubFileInfo();
    fileInfo.setReferencedByAsset(dataset);
    BucketStorageLocation location = new BucketStorageLocation();
    location.setStorageBucket("test-bucket");
    location.setStorageKey("test-key");
    fileInfo.setBucketStorageLocation(location);

    Mockito.when(
            mockEntityService.getLatestAspect(
                Mockito.any(OperationContext.class),
                eq(fileUrn),
                eq(Constants.DATAHUB_FILE_INFO_ASPECT_NAME)))
        .thenReturn(fileInfo);

    // No other relationships
    Mockito.when(
            _graphService.scrollRelatedEntities(
                any(OperationContext.class),
                nullable(Set.class),
                eq(newFilter("urn", dataset.toString())),
                nullable(Set.class),
                eq(EMPTY_FILTER),
                eq(ImmutableSet.of()),
                eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
                eq(Edge.EDGE_SORT_CRITERION),
                nullable(String.class),
                eq("5m"),
                eq(1000),
                nullable(Long.class),
                nullable(Long.class)))
        .thenReturn(emptyScrollGraphResult());

    final DeleteReferencesResponse response =
        deleteEntityService.deleteReferencesTo(opContext, dataset, false);

    // Verify file entity was still soft-deleted even without S3Util
    Mockito.verify(mockEntityService, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.any(MetadataChangeProposal.class),
            Mockito.any(AuditStamp.class),
            eq(true));
  }

  /**
   * Test that file cleanup continues even if S3 deletion fails. We soft-delete the entity to avoid
   * leaving the parent entity in limbo. The tradeoff is accepting a potential orphaned S3 object.
   */
  @Test
  public void testDeleteFileReferencesWithS3Failure() {
    EntityService<?> mockEntityService = Mockito.mock(EntityService.class);
    S3Util mockS3Util = Mockito.mock(S3Util.class);
    DeleteEntityService deleteEntityService =
        new DeleteEntityService(mockEntityService, _graphService, _mockSearchService, mockS3Util);

    final Urn dataset = UrnUtils.toDatasetUrn("snowflake", "test", "DEV");
    final Urn fileUrn = UrnUtils.getUrn("urn:li:dataHubFile:test-file-id");

    // Mock file search result
    ScrollResult fileScrollResult = new ScrollResult();
    SearchEntityArray fileEntities = new SearchEntityArray();
    SearchEntity fileEntity = new SearchEntity();
    fileEntity.setEntity(fileUrn);
    fileEntities.add(fileEntity);
    fileScrollResult.setEntities(fileEntities);
    fileScrollResult.setNumEntities(1);
    fileScrollResult.setScrollId("1");

    Mockito.when(
            _mockSearchService.structuredScroll(
                Mockito.any(OperationContext.class),
                Mockito.argThat(
                    set ->
                        set != null
                            && set.size() == 1
                            && set.contains(Constants.DATAHUB_FILE_ENTITY_NAME)),
                Mockito.eq("*"),
                Mockito.any(Filter.class),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq("5m"),
                Mockito.eq(1000)))
        .thenReturn(fileScrollResult);

    ScrollResult emptyScrollResult = new ScrollResult();
    emptyScrollResult.setNumEntities(0);
    Mockito.when(
            _mockSearchService.structuredScroll(
                Mockito.any(OperationContext.class),
                Mockito.argThat(
                    set ->
                        set != null
                            && set.size() == 1
                            && set.contains(Constants.DATAHUB_FILE_ENTITY_NAME)),
                Mockito.eq("*"),
                Mockito.any(Filter.class),
                Mockito.eq(null),
                Mockito.eq("1"),
                Mockito.eq("5m"),
                Mockito.eq(1000)))
        .thenReturn(emptyScrollResult);

    // Mock file info aspect
    DataHubFileInfo fileInfo = new DataHubFileInfo();
    fileInfo.setReferencedByAsset(dataset);
    BucketStorageLocation location = new BucketStorageLocation();
    location.setStorageBucket("test-bucket");
    location.setStorageKey("test-key");
    fileInfo.setBucketStorageLocation(location);

    Mockito.when(
            mockEntityService.getLatestAspect(
                Mockito.any(OperationContext.class),
                eq(fileUrn),
                eq(Constants.DATAHUB_FILE_INFO_ASPECT_NAME)))
        .thenReturn(fileInfo);

    // Make S3 deletion throw an exception
    Mockito.doThrow(new RuntimeException("S3 error"))
        .when(mockS3Util)
        .deleteObject("test-bucket", "test-key");

    // No other relationships
    Mockito.when(
            _graphService.scrollRelatedEntities(
                any(OperationContext.class),
                nullable(Set.class),
                eq(newFilter("urn", dataset.toString())),
                nullable(Set.class),
                eq(EMPTY_FILTER),
                eq(ImmutableSet.of()),
                eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
                eq(Edge.EDGE_SORT_CRITERION),
                nullable(String.class),
                eq("5m"),
                eq(1000),
                nullable(Long.class),
                nullable(Long.class)))
        .thenReturn(emptyScrollGraphResult());

    // Operation should succeed despite S3 failure
    final DeleteReferencesResponse response =
        deleteEntityService.deleteReferencesTo(opContext, dataset, false);

    // Verify S3 delete was attempted
    Mockito.verify(mockS3Util, Mockito.times(1)).deleteObject("test-bucket", "test-key");

    // Verify file entity was still soft-deleted despite S3 failure to avoid leaving entity in limbo
    Mockito.verify(mockEntityService, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.any(MetadataChangeProposal.class),
            Mockito.any(AuditStamp.class),
            eq(true));

    // Verify the response includes the file count
    assertEquals(1, (int) response.getTotal());
  }

  /**
   * Tests that deleteReferencesTo processes ALL related entities across multiple scroll pages.
   * Simulates a real-world scenario where scroll-based pagination returns results in batches via
   * PIT + search_after cursors.
   *
   * <p>This verifies the fix for a bug where offset-based pagination caused stalls when deleting
   * entities referenced by >5000 entities, as graph edge deletions between iterations shifted the
   * result window.
   */
  @Test
  public void testDeleteReferencesToProcessesAllBatchesWhenTotalExceedsBatchSize()
      throws URISyntaxException {
    EntityService<?> mockEntityService = Mockito.mock(EntityService.class);
    EntitySearchService mockSearchService = Mockito.mock(EntitySearchService.class);
    GraphService mockGraphService = Mockito.mock(GraphService.class);
    DeleteEntityService deleteEntityService =
        new DeleteEntityService(mockEntityService, mockGraphService, mockSearchService, null);

    final Urn tagUrn = UrnUtils.getUrn("urn:li:tag:stress-hot-tag-1");
    final String tagUrnStr = tagUrn.toString();

    // Create two batches of related entities (simulating scroll pages)
    ImmutableList<RelatedEntities> batch1Entities =
        ImmutableList.of(
            incomingRelation(
                "TaggedWith",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t1,PROD)",
                tagUrnStr),
            incomingRelation(
                "TaggedWith",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t2,PROD)",
                tagUrnStr),
            incomingRelation(
                "TaggedWith",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t3,PROD)",
                tagUrnStr));

    ImmutableList<RelatedEntities> batch2Entities =
        ImmutableList.of(
            incomingRelation(
                "TaggedWith",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t4,PROD)",
                tagUrnStr),
            incomingRelation(
                "TaggedWith",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t5,PROD)",
                tagUrnStr),
            incomingRelation(
                "TaggedWith",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t6,PROD)",
                tagUrnStr));

    // First scroll page: returns batch1 (3 entities), total=6, scrollId to continue
    RelatedEntitiesScrollResult firstResult = scrollGraphResult(6, "scroll-1", batch1Entities);

    // Second scroll page: returns batch2 (3 entities), no more pages (scrollId=null)
    RelatedEntitiesScrollResult secondResult = scrollGraphResult(6, null, batch2Entities);

    Mockito.when(
            mockGraphService.scrollRelatedEntities(
                any(OperationContext.class),
                nullable(Set.class),
                eq(newFilter("urn", tagUrnStr)),
                nullable(Set.class),
                eq(EMPTY_FILTER),
                eq(ImmutableSet.of()),
                eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
                eq(Edge.EDGE_SORT_CRITERION),
                nullable(String.class),
                eq("5m"),
                eq(1000),
                nullable(Long.class),
                nullable(Long.class)))
        .thenReturn(firstResult, secondResult);

    // Mock empty results for search-based references (files, subscriptions, forms)
    ScrollResult emptyScrollResult = new ScrollResult();
    emptyScrollResult.setEntities(new SearchEntityArray());
    emptyScrollResult.setNumEntities(0);
    Mockito.when(
            mockSearchService.structuredScroll(
                Mockito.any(OperationContext.class),
                Mockito.any(),
                Mockito.eq("*"),
                Mockito.any(Filter.class),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.anyInt()))
        .thenReturn(emptyScrollResult);

    // Mock entity lookups for getRelatedAspectStream (called during initial result processing).
    // Return null to simulate entities that were concurrently deleted during cascade.
    Mockito.when(
            mockEntityService.getEntityV2(
                any(OperationContext.class),
                Mockito.anyString(),
                Mockito.any(Urn.class),
                Mockito.anySet()))
        .thenReturn(null);

    final DeleteReferencesResponse response =
        deleteEntityService.deleteReferencesTo(opContext, tagUrn, false);

    // The critical assertion: the total reported should include ALL entities (6)
    assertEquals(6, (int) response.getTotal());

    // Capture scrollId arguments to verify cursor forwarding between pages
    @SuppressWarnings("unchecked")
    ArgumentCaptor<String> scrollIdCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.verify(mockGraphService, Mockito.times(2))
        .scrollRelatedEntities(
            any(OperationContext.class),
            nullable(Set.class),
            eq(newFilter("urn", tagUrnStr)),
            nullable(Set.class),
            eq(EMPTY_FILTER),
            eq(ImmutableSet.of()),
            eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
            eq(Edge.EDGE_SORT_CRITERION),
            scrollIdCaptor.capture(),
            eq("5m"),
            eq(1000),
            nullable(Long.class),
            nullable(Long.class));

    // 1st call: scrollId=null (initial fetch)
    // 2nd call: scrollId="scroll-1" (forwarded from first result)
    List<String> capturedScrollIds = scrollIdCaptor.getAllValues();
    assertEquals(2, capturedScrollIds.size());
    assertEquals(null, capturedScrollIds.get(0));
    assertEquals("scroll-1", capturedScrollIds.get(1));
  }

  /**
   * Tests that errors during reference deletion are logged with structured context via
   * handleError(), not silently swallowed.
   *
   * <p>handleError() was a NO-OP since the class was created. This was never discovered because:
   *
   * <ul>
   *   <li>It is private with zero observable side effects — no test could detect its absence
   *   <li>No test triggered an error path through deleteReference() — all tests used happy paths
   *   <li>At small scale (&lt;5000 entities) cascades succeed even with some silent failures
   *   <li>The fire-and-forget async pattern in GraphQL resolvers means errors never propagate to
   *       callers — the mutation returns true before the cascade starts
   *   <li>The 5 callers of handleError() each have their own log.error() before calling it, so
   *       individual errors DO appear in logs — but the structured error context (reason enum,
   *       entity URNs, aspect names) passed to handleError() is discarded, making it impossible to
   *       aggregate, alert on, or count reference cleanup failures
   * </ul>
   */
  @Test
  public void testHandleErrorLogsStructuredContext() throws URISyntaxException {
    EntityService<?> mockEntityService = Mockito.mock(EntityService.class);
    EntitySearchService mockSearchService = Mockito.mock(EntitySearchService.class);
    GraphService mockGraphService = Mockito.mock(GraphService.class);
    DeleteEntityService deleteEntityService =
        new DeleteEntityService(mockEntityService, mockGraphService, mockSearchService, null);

    final Urn tagUrn = UrnUtils.getUrn("urn:li:tag:orphan-test");

    // One related entity that will hit the error path
    final String tagUrnStr = tagUrn.toString();
    ImmutableList<RelatedEntities> entities =
        ImmutableList.of(
            incomingRelation(
                "TaggedWith",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t1,PROD)",
                tagUrnStr));

    RelatedEntitiesScrollResult relatedResult = scrollGraphResult(1, null, entities);

    Mockito.when(
            mockGraphService.scrollRelatedEntities(
                any(OperationContext.class),
                nullable(Set.class),
                eq(newFilter("urn", tagUrnStr)),
                nullable(Set.class),
                eq(EMPTY_FILTER),
                eq(ImmutableSet.of()),
                eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
                eq(Edge.EDGE_SORT_CRITERION),
                nullable(String.class),
                eq("5m"),
                eq(1000),
                nullable(Long.class),
                nullable(Long.class)))
        .thenReturn(relatedResult);

    ScrollResult emptyScrollResult = new ScrollResult();
    emptyScrollResult.setEntities(new SearchEntityArray());
    emptyScrollResult.setNumEntities(0);
    Mockito.when(
            mockSearchService.structuredScroll(
                Mockito.any(OperationContext.class),
                Mockito.any(),
                Mockito.eq("*"),
                Mockito.any(Filter.class),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.anyInt()))
        .thenReturn(emptyScrollResult);

    // getEntityV2 returns null → getAspectsReferringTo returns empty → handleError called
    // with ENTITY_SERVICE_ASPECT_NOT_FOUND
    Mockito.when(
            mockEntityService.getEntityV2(
                any(OperationContext.class),
                Mockito.anyString(),
                Mockito.any(Urn.class),
                Mockito.anySet()))
        .thenReturn(null);

    // Capture logs to verify handleError produces a WARN with structured error context
    ch.qos.logback.classic.Logger serviceLogger =
        (ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger(DeleteEntityService.class);
    ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> logAppender =
        new ch.qos.logback.core.read.ListAppender<>();
    logAppender.start();
    serviceLogger.addAppender(logAppender);

    try {
      deleteEntityService.deleteReferencesTo(opContext, tagUrn, false);

      // handleError should log at WARN with the error reason
      boolean foundHandleErrorLog =
          logAppender.list.stream()
              .anyMatch(
                  event ->
                      event.getLevel() == ch.qos.logback.classic.Level.WARN
                          && event.getFormattedMessage() != null
                          && event.getFormattedMessage().contains("deleteReference error"));
      assertTrue(
          foundHandleErrorLog,
          "Expected handleError to log a WARN containing 'deleteReference error'. "
              + "Actual WARN/ERROR messages: "
              + logAppender.list.stream()
                  .filter(
                      e ->
                          e.getLevel() == ch.qos.logback.classic.Level.WARN
                              || e.getLevel() == ch.qos.logback.classic.Level.ERROR)
                  .map(e -> e.getLevel() + ": " + e.getFormattedMessage())
                  .collect(java.util.stream.Collectors.joining("\n")));
    } finally {
      serviceLogger.detachAppender(logAppender);
    }
  }
}
