package com.linkedin.metadata.entity;

import static com.linkedin.metadata.search.utils.QueryUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableList;
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
import com.linkedin.metadata.Constants;
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
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
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
        new DeleteEntityService(_entityServiceImpl, _graphService, _mockSearchService);
  }

  /**
   * This test checks whether deleting non array references in PDL aspects generates a valid MCP.
   */
  @Test
  public void testDeleteUniqueRefGeneratesValidMCP() {
    final Urn dataset = UrnUtils.toDatasetUrn("snowflake", "test", "DEV");
    final Urn container = UrnUtils.getUrn("urn:li:container:d1006cf3-3ff9-48e3-85cd-26eb23775ab2");

    final RelatedEntitiesResult mockRelatedEntities =
        new RelatedEntitiesResult(
            0, 1, 1, ImmutableList.of(new RelatedEntity("IsPartOf", dataset.toString())));

    Mockito.when(
            _graphService.findRelatedEntities(
                any(OperationContext.class),
                nullable(List.class),
                eq(newFilter("urn", container.toString())),
                nullable(List.class),
                eq(EMPTY_FILTER),
                eq(ImmutableList.of()),
                eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
                eq(0),
                eq((10000))))
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
            container,
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
        .thenReturn(result);

    final DeleteReferencesResponse response =
        _deleteEntityService.deleteReferencesTo(opContext, container, false);
    assertEquals(1, (int) response.getTotal());
    assertFalse(response.getRelatedAspects().isEmpty());
  }

  /** This test checks whether updating search references works properly (for forms only for now) */
  @Test
  public void testDeleteSearchReferences() {
    EntityService<?> mockEntityService = Mockito.mock(EntityService.class);
    DeleteEntityService deleteEntityService =
        new DeleteEntityService(mockEntityService, _graphService, _mockSearchService);

    final Urn dataset = UrnUtils.toDatasetUrn("snowflake", "test", "DEV");
    final Urn form = UrnUtils.getUrn("urn:li:form:12345");

    ScrollResult scrollResult = new ScrollResult();
    SearchEntityArray entities = new SearchEntityArray();
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(dataset);
    entities.add(searchEntity);
    scrollResult.setEntities(entities);
    scrollResult.setNumEntities(1);
    scrollResult.setScrollId("1");
    Mockito.when(
            _mockSearchService.structuredScroll(
                Mockito.any(OperationContext.class),
                Mockito.any(),
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
            _mockSearchService.structuredScroll(
                Mockito.any(OperationContext.class),
                Mockito.any(),
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
                nullable(List.class),
                eq(newFilter("urn", form.toString())),
                nullable(List.class),
                eq(EMPTY_FILTER),
                eq(ImmutableList.of()),
                eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
                eq(0),
                eq((10000))))
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
    DeleteEntityService deleteEntityService =
        new DeleteEntityService(mockEntityService, _graphService, _mockSearchService);

    final Urn dataset = UrnUtils.toDatasetUrn("snowflake", "test", "DEV");
    final Urn form = UrnUtils.getUrn("urn:li:form:12345");

    ScrollResult scrollResult = new ScrollResult();
    scrollResult.setEntities(new SearchEntityArray());
    scrollResult.setNumEntities(0);
    Mockito.when(
            _mockSearchService.structuredScroll(
                Mockito.any(OperationContext.class),
                Mockito.any(),
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
                nullable(List.class),
                eq(newFilter("urn", form.toString())),
                nullable(List.class),
                eq(EMPTY_FILTER),
                eq(ImmutableList.of()),
                eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
                eq(0),
                eq((10000))))
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
    DeleteEntityService deleteEntityService =
        new DeleteEntityService(mockEntityService, _graphService, _mockSearchService);

    final Urn dataset = UrnUtils.toDatasetUrn("snowflake", "test", "DEV");
    final Urn form = UrnUtils.getUrn("urn:li:form:12345");

    ScrollResult scrollResult = new ScrollResult();
    SearchEntityArray entities = new SearchEntityArray();
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(dataset);
    entities.add(searchEntity);
    scrollResult.setEntities(entities);
    scrollResult.setNumEntities(1);
    scrollResult.setScrollId("1");
    Mockito.when(
            _mockSearchService.structuredScroll(
                Mockito.any(OperationContext.class),
                Mockito.any(),
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
                nullable(List.class),
                eq(newFilter("urn", form.toString())),
                nullable(List.class),
                eq(EMPTY_FILTER),
                eq(ImmutableList.of()),
                eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
                eq(0),
                eq((10000))))
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
}
