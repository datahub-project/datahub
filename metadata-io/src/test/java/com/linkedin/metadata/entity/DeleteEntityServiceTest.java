package com.linkedin.metadata.entity;

import static com.linkedin.metadata.search.utils.QueryUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.container.Container;
import com.linkedin.entity.EntityResponse;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.graph.RelatedEntity;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.run.DeleteReferencesResponse;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import java.sql.Timestamp;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DeleteEntityServiceTest {

  protected EbeanAspectDao _aspectDao;

  protected EntityServiceImpl _entityServiceImpl;
  protected GraphService _graphService = Mockito.mock(GraphService.class);

  protected DeleteEntityService _deleteEntityService;
  protected UpdateIndicesService _mockUpdateIndicesService;

  protected EntityRegistry _entityRegistry;

  public DeleteEntityServiceTest() {
    _entityRegistry =
        new ConfigEntityRegistry(
            Snapshot.class.getClassLoader().getResourceAsStream("entity-registry.yml"));
    _aspectDao = mock(EbeanAspectDao.class);
    _mockUpdateIndicesService = mock(UpdateIndicesService.class);
    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    _entityServiceImpl =
        new EntityServiceImpl(
            _aspectDao,
            mock(EventProducer.class),
            _entityRegistry,
            true,
            _mockUpdateIndicesService,
            preProcessHooks);
    _deleteEntityService = new DeleteEntityService(_entityServiceImpl, _graphService);
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
                null,
                newFilter("urn", container.toString()),
                null,
                EMPTY_FILTER,
                ImmutableList.of(),
                newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING),
                0,
                10000))
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
    Mockito.when(_aspectDao.batchGet(Mockito.any())).thenReturn(dbEntries);

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
        _deleteEntityService.deleteReferencesTo(container, false);
    assertEquals(1, (int) response.getTotal());
    assertFalse(response.getRelatedAspects().isEmpty());
  }
}
