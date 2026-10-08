package com.linkedin.metadata.client;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RequiredFieldNotPresentException;
import com.linkedin.domain.Domains;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.DeleteCeiling;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.HardDeleteService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.RollbackResult;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.entity.UpdateAspectResult;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.ebean.batch.ProposedItem;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.service.HardDeleteDispatcher;
import com.linkedin.metadata.service.HardDeleteRequest;
import com.linkedin.metadata.service.RollbackService;
import com.linkedin.metadata.service.async.delete.ReliableHardDelete;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class JavaEntityClientTest {

  private EntityService<?> _entityService;
  private DeleteEntityService _deleteEntityService;
  private EntitySearchService _entitySearchService;
  private CachingEntitySearchService _cachingEntitySearchService;
  private SearchService _searchService;
  private LineageSearchService _lineageSearchService;
  private TimeseriesAspectService _timeseriesAspectService;
  private EventProducer _eventProducer;
  private MetricUtils _metricUtils;
  private RollbackService rollbackService;
  private OperationContext opContext;

  @BeforeMethod
  public void setupTest() {
    _entityService = mock(EntityService.class);
    _deleteEntityService = mock(DeleteEntityService.class);
    _entitySearchService = mock(EntitySearchService.class);
    _cachingEntitySearchService = mock(CachingEntitySearchService.class);
    _searchService = mock(SearchService.class);
    _lineageSearchService = mock(LineageSearchService.class);
    _timeseriesAspectService = mock(TimeseriesAspectService.class);
    rollbackService = mock(RollbackService.class);
    _eventProducer = mock(EventProducer.class);
    _metricUtils = mock(MetricUtils.class);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  private JavaEntityClient getJavaEntityClient() {
    return new JavaEntityClient(
        _entityService,
        _deleteEntityService,
        _entitySearchService,
        _cachingEntitySearchService,
        _searchService,
        _lineageSearchService,
        _timeseriesAspectService,
        rollbackService,
        _eventProducer,
        EntityClientConfig.builder().batchGetV2Size(1).build(),
        _metricUtils);
  }

  private JavaEntityClient getJavaEntityClient(HardDeleteService hardDeleteService) {
    return new JavaEntityClient(
        _entityService,
        _deleteEntityService,
        _entitySearchService,
        _cachingEntitySearchService,
        _searchService,
        _lineageSearchService,
        _timeseriesAspectService,
        rollbackService,
        _eventProducer,
        EntityClientConfig.builder().batchGetV2Size(1).build(),
        _metricUtils,
        hardDeleteService);
  }

  private HardDeleteService hardDeleteService(
      final boolean reliableHardDelete, final HardDeleteDispatcher dispatcher) {
    return new HardDeleteService(
        _entityService,
        _deleteEntityService,
        _timeseriesAspectService,
        new ReliableHardDelete(_entityService, reliableHardDelete),
        dispatcher);
  }

  /** Without the service (the system client): today's code. */
  @Test
  void testDeletesWithoutTheServiceRunTodaysCode() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:tag:noService");
    JavaEntityClient client = getJavaEntityClient();

    client.deleteEntity(opContext, urn);
    client.deleteEntityReferences(opContext, urn);

    verify(_entityService).deleteUrn(opContext, urn);
    verify(_deleteEntityService).deleteReferencesTo(opContext, urn, false);
  }

  @Test
  void testDeleteEntityDelegatesToTheService() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:tag:delegates");
    HardDeleteService hardDeleteService = mock(HardDeleteService.class);

    getJavaEntityClient(hardDeleteService).deleteEntity(opContext, urn);

    verify(hardDeleteService).deleteEntity(opContext, urn);
    verify(_entityService, never()).deleteUrn(any(OperationContext.class), any(Urn.class));
  }

  @Test
  void testDeleteEntityFailureDoesNotFallBackToInlineDelete() {
    Urn urn = UrnUtils.getUrn("urn:li:tag:serviceFails");
    HardDeleteService hardDeleteService = mock(HardDeleteService.class);
    when(hardDeleteService.deleteEntity(opContext, urn))
        .thenThrow(new IllegalStateException("failed"));

    JavaEntityClient client = getJavaEntityClient(hardDeleteService);

    assertThrows(IllegalStateException.class, () -> client.deleteEntity(opContext, urn));
    verify(_entityService, never()).deleteUrn(any(OperationContext.class), any(Urn.class));
  }

  /** Declined: the reference cleanup runs here as today (a failure is run again), offered once. */
  @Test
  void testDeleteEntityReferencesDeclinedRunsTodaysCodeAfterOneOffer() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:tag:referencesDeclined");
    HardDeleteDispatcher dispatcher = mock(HardDeleteDispatcher.class);
    when(_deleteEntityService.deleteReferencesTo(opContext, urn, false))
        .thenThrow(new IllegalArgumentException("transient"))
        .thenReturn(null);

    getJavaEntityClient(hardDeleteService(true, dispatcher)).deleteEntityReferences(opContext, urn);

    verify(dispatcher, times(1)).dispatch(opContext, HardDeleteRequest.references(urn));
    verify(_deleteEntityService, times(2)).deleteReferencesTo(opContext, urn, false);
  }

  /**
   * Declined: the entity is deleted now and the references when the caller runs them, with today's
   * code; the combined delete is offered once and the references never on their own.
   */
  @Test
  void testDeleteEntityThenReferencesDeclinedRunsBothHereAfterOneOffer() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:tag:bothDeclined");
    DeleteCeiling ceiling = new DeleteCeiling(Map.of("tagKey", 1L), 1L);
    HardDeleteDispatcher dispatcher = mock(HardDeleteDispatcher.class);
    when(_entityService.captureDeleteCeiling(opContext, urn)).thenReturn(Optional.of(ceiling));
    when(_entityService.deleteUrn(opContext, urn, ceiling))
        .thenReturn(
            new RollbackRunResult(
                List.of(),
                1,
                List.of(
                    new RollbackResult(
                        urn,
                        "tag",
                        "tagKey",
                        null,
                        null,
                        null,
                        null,
                        ChangeType.DELETE,
                        true,
                        0))));
    when(_deleteEntityService.deleteReferencesTo(opContext, urn, false))
        .thenThrow(new IllegalArgumentException("transient"))
        .thenReturn(null);

    EntityClient.ReferencesCleanup references =
        getJavaEntityClient(hardDeleteService(true, dispatcher))
            .deleteEntityThenReferences(opContext, urn);

    verify(_entityService).deleteUrn(opContext, urn, ceiling);
    verifyNoInteractions(_deleteEntityService);
    references.run();
    verify(dispatcher, times(1)).dispatch(any(), any());
    verify(dispatcher).dispatch(opContext, HardDeleteRequest.entityAndReferences(urn, ceiling));
    verify(_deleteEntityService, times(2)).deleteReferencesTo(opContext, urn, false);
  }

  /** Taken: nothing runs here, now or when the caller runs the cleanup. */
  @Test
  void testDeleteEntityThenReferencesTakenRunsNothingHere() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:tag:bothTaken");
    DeleteCeiling ceiling = new DeleteCeiling(Map.of("tagKey", 1L), 1L);
    HardDeleteDispatcher dispatcher = mock(HardDeleteDispatcher.class);
    when(dispatcher.dispatch(any(), any())).thenReturn(true);
    when(_entityService.captureDeleteCeiling(opContext, urn)).thenReturn(Optional.of(ceiling));

    getJavaEntityClient(hardDeleteService(false, dispatcher))
        .deleteEntityThenReferences(opContext, urn)
        .run();

    verify(_entityService, never()).deleteUrn(any(OperationContext.class), any(Urn.class));
    verifyNoInteractions(_deleteEntityService);
  }

  @Test
  void testSuccessWithNoRetries() {
    JavaEntityClient client = getJavaEntityClient();
    Supplier<Object> mockSupplier = mock(Supplier.class);

    when(mockSupplier.get()).thenReturn(42);

    assertEquals(client.withRetry(mockSupplier, null), 42);
    verify(mockSupplier, times(1)).get();
    verify(_metricUtils, times(0)).increment(any(), anyDouble());
  }

  @Test
  void testSuccessAfterMultipleRetries() {
    JavaEntityClient client = getJavaEntityClient();
    Supplier<Object> mockSupplier = mock(Supplier.class);
    Exception e = new IllegalArgumentException();

    when(mockSupplier.get()).thenThrow(e).thenThrow(e).thenThrow(e).thenReturn(42);

    assertEquals(client.withRetry(mockSupplier, "test"), 42);
    verify(mockSupplier, times(4)).get();
    verify(_metricUtils, times(3))
        .increment(eq(client.getClass()), eq("test_exception_" + e.getClass().getName()), eq(1d));
  }

  @Test
  void testThrowAfterMultipleRetries() {
    JavaEntityClient client = getJavaEntityClient();
    Supplier<Object> mockSupplier = mock(Supplier.class);
    Exception e = new IllegalArgumentException();

    when(mockSupplier.get()).thenThrow(e).thenThrow(e).thenThrow(e).thenThrow(e);

    assertThrows(IllegalArgumentException.class, () -> client.withRetry(mockSupplier, "test"));
    verify(mockSupplier, times(4)).get();
    verify(_metricUtils, times(4))
        .increment(eq(client.getClass()), eq("test_exception_" + e.getClass().getName()), eq(1d));
  }

  @Test
  void testThrowAfterNonRetryableException() {
    JavaEntityClient client = getJavaEntityClient();
    Supplier<Object> mockSupplier = mock(Supplier.class);
    Exception e = new RequiredFieldNotPresentException("test");

    when(mockSupplier.get()).thenThrow(e);

    assertThrows(
        RequiredFieldNotPresentException.class, () -> client.withRetry(mockSupplier, null));
    verify(mockSupplier, times(1)).get();
    verify(_metricUtils, times(1))
        .increment(eq(client.getClass()), eq("exception_" + e.getClass().getName()), eq(1d));
  }

  @Test
  void tesIngestOrderingWithProposedItem() throws RemoteInvocationException {
    JavaEntityClient client = getJavaEntityClient();
    Urn testUrn = UrnUtils.getUrn("urn:li:container:orderingTest");
    AuditStamp auditStamp = AuditStampUtils.createDefaultAuditStamp();
    MetadataChangeProposal mcp =
        new MetadataChangeProposal()
            .setEntityUrn(testUrn)
            .setAspectName("status")
            .setEntityType("container")
            .setChangeType(ChangeType.UPSERT)
            .setAspect(GenericRecordUtils.serializeAspect(new Status().setRemoved(true)));

    when(_entityService.ingestProposal(
            any(OperationContext.class), any(AspectsBatch.class), eq(false)))
        .thenReturn(
            List.<IngestResult>of(
                // Misc - unrelated urn
                IngestResult.builder()
                    .urn(UrnUtils.getUrn("urn:li:container:domains"))
                    .request(
                        ChangeItemImpl.builder()
                            .entitySpec(
                                opContext
                                    .getEntityRegistry()
                                    .getEntitySpec(Constants.CONTAINER_ENTITY_NAME))
                            .aspectSpec(
                                opContext
                                    .getEntityRegistry()
                                    .getEntitySpec(Constants.CONTAINER_ENTITY_NAME)
                                    .getAspectSpec(Constants.DOMAINS_ASPECT_NAME))
                            .changeType(ChangeType.UPSERT)
                            .urn(UrnUtils.getUrn("urn:li:container:domains"))
                            .aspectName("domains")
                            .recordTemplate(new Domains().setDomains(new UrnArray()))
                            .auditStamp(auditStamp)
                            .build(opContext.getAspectRetriever()))
                    .isUpdate(true)
                    .publishedMCL(true)
                    .sqlCommitted(true)
                    .build(),
                // Side effect - unrelated urn
                IngestResult.builder()
                    .urn(UrnUtils.getUrn("urn:li:container:sideEffect"))
                    .request(
                        ChangeItemImpl.builder()
                            .entitySpec(
                                opContext
                                    .getEntityRegistry()
                                    .getEntitySpec(Constants.CONTAINER_ENTITY_NAME))
                            .aspectSpec(
                                opContext
                                    .getEntityRegistry()
                                    .getEntitySpec(Constants.CONTAINER_ENTITY_NAME)
                                    .getAspectSpec(Constants.STATUS_ASPECT_NAME))
                            .changeType(ChangeType.UPSERT)
                            .urn(UrnUtils.getUrn("urn:li:container:sideEffect"))
                            .aspectName("status")
                            .recordTemplate(new Status().setRemoved(false))
                            .auditStamp(auditStamp)
                            .build(opContext.getAspectRetriever()))
                    .isUpdate(true)
                    .publishedMCL(true)
                    .sqlCommitted(true)
                    .build(),
                // Expected response
                IngestResult.builder()
                    .urn(testUrn)
                    .request(
                        ProposedItem.builder()
                            .build(mcp, auditStamp, opContext.getEntityRegistry()))
                    .result(UpdateAspectResult.builder().mcp(mcp).urn(testUrn).build())
                    .isUpdate(true)
                    .publishedMCL(true)
                    .sqlCommitted(true)
                    .build()));

    String urnStr = client.ingestProposal(opContext, mcp, false);

    assertEquals(urnStr, "urn:li:container:orderingTest");
  }
}
