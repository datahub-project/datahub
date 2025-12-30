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
import com.linkedin.metadata.aspect.models.graph.RelatedEntity;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
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
import java.sql.Timestamp;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

    final RelatedEntitiesResult mockRelatedEntities =
        new RelatedEntitiesResult(
            0, 1, 1, ImmutableList.of(new RelatedEntity("IsPartOf", dataset.toString())));

    Mockito.when(
            _graphService.findRelatedEntities(
                any(OperationContext.class),
                nullable(Set.class),
                eq(newFilter("urn", container.toString())),
                nullable(Set.class),
                eq(EMPTY_FILTER),
                eq(ImmutableSet.of()),
                eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
                eq(0),
                nullable(Integer.class)))
        .thenReturn(mockRelatedEntities);

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
    final RelatedEntitiesResult mockRelatedEntities =
        new RelatedEntitiesResult(0, 0, 0, ImmutableList.of());
    Mockito.when(
            _graphService.findRelatedEntities(
                any(OperationContext.class),
                nullable(Set.class),
                eq(newFilter("urn", form.toString())),
                nullable(Set.class),
                eq(EMPTY_FILTER),
                eq(ImmutableSet.of()),
                eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
                eq(0),
                nullable(Integer.class)))
        .thenReturn(mockRelatedEntities);

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
    final RelatedEntitiesResult mockRelatedEntities =
        new RelatedEntitiesResult(0, 0, 0, ImmutableList.of());
    Mockito.when(
            _graphService.findRelatedEntities(
                any(OperationContext.class),
                nullable(Set.class),
                eq(newFilter("urn", form.toString())),
                nullable(Set.class),
                eq(EMPTY_FILTER),
                eq(ImmutableSet.of()),
                eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
                eq(0),
                nullable(Integer.class)))
        .thenReturn(mockRelatedEntities);

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
    final RelatedEntitiesResult mockRelatedEntities =
        new RelatedEntitiesResult(0, 0, 0, ImmutableList.of());
    Mockito.when(
            _graphService.findRelatedEntities(
                any(OperationContext.class),
                nullable(Set.class),
                eq(newFilter("urn", form.toString())),
                nullable(Set.class),
                eq(EMPTY_FILTER),
                eq(ImmutableSet.of()),
                eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
                eq(0),
                nullable(Integer.class)))
        .thenReturn(mockRelatedEntities);

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
    final RelatedEntitiesResult mockRelatedEntities =
        new RelatedEntitiesResult(0, 0, 0, ImmutableList.of());
    Mockito.when(
            _graphService.findRelatedEntities(
                any(OperationContext.class),
                nullable(Set.class),
                eq(newFilter("urn", dataset.toString())),
                nullable(Set.class),
                eq(EMPTY_FILTER),
                eq(ImmutableSet.of()),
                eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
                eq(0),
                nullable(Integer.class)))
        .thenReturn(mockRelatedEntities);

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
    final RelatedEntitiesResult mockRelatedEntities =
        new RelatedEntitiesResult(0, 0, 0, ImmutableList.of());
    Mockito.when(
            _graphService.findRelatedEntities(
                any(OperationContext.class),
                nullable(Set.class),
                eq(newFilter("urn", dataset.toString())),
                nullable(Set.class),
                eq(EMPTY_FILTER),
                eq(ImmutableSet.of()),
                eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
                eq(0),
                nullable(Integer.class)))
        .thenReturn(mockRelatedEntities);

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
    final RelatedEntitiesResult mockRelatedEntities =
        new RelatedEntitiesResult(0, 0, 0, ImmutableList.of());
    Mockito.when(
            _graphService.findRelatedEntities(
                any(OperationContext.class),
                nullable(Set.class),
                eq(newFilter("urn", dataset.toString())),
                nullable(Set.class),
                eq(EMPTY_FILTER),
                eq(ImmutableSet.of()),
                eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
                eq(0),
                nullable(Integer.class)))
        .thenReturn(mockRelatedEntities);

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
}
