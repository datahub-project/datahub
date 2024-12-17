package com.linkedin.metadata.client;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.codahale.metrics.Counter;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RequiredFieldNotPresentException;
import com.linkedin.domain.Domains;
import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.UpdateAspectResult;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.ebean.batch.ProposedItem;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.service.RollbackService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.function.Supplier;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterMethod;
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
  private MockedStatic<MetricUtils> _metricUtils;
  private RollbackService rollbackService;
  private Counter _counter;
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
    _metricUtils = mockStatic(MetricUtils.class);
    _counter = mock(Counter.class);
    when(MetricUtils.counter(any(), any())).thenReturn(_counter);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @AfterMethod
  public void closeTest() {
    _metricUtils.close();
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
        EntityClientConfig.builder().batchGetV2Size(1).build());
  }

  @Test
  void testSuccessWithNoRetries() {
    JavaEntityClient client = getJavaEntityClient();
    Supplier<Object> mockSupplier = mock(Supplier.class);

    when(mockSupplier.get()).thenReturn(42);

    assertEquals(client.withRetry(mockSupplier, null), 42);
    verify(mockSupplier, times(1)).get();
    _metricUtils.verify(() -> MetricUtils.counter(any(), any()), times(0));
  }

  @Test
  void testSuccessAfterMultipleRetries() {
    JavaEntityClient client = getJavaEntityClient();
    Supplier<Object> mockSupplier = mock(Supplier.class);
    Exception e = new IllegalArgumentException();

    when(mockSupplier.get()).thenThrow(e).thenThrow(e).thenThrow(e).thenReturn(42);

    assertEquals(client.withRetry(mockSupplier, "test"), 42);
    verify(mockSupplier, times(4)).get();
    _metricUtils.verify(
        () -> MetricUtils.counter(client.getClass(), "test_exception_" + e.getClass().getName()),
        times(3));
  }

  @Test
  void testThrowAfterMultipleRetries() {
    JavaEntityClient client = getJavaEntityClient();
    Supplier<Object> mockSupplier = mock(Supplier.class);
    Exception e = new IllegalArgumentException();

    when(mockSupplier.get()).thenThrow(e).thenThrow(e).thenThrow(e).thenThrow(e);

    assertThrows(IllegalArgumentException.class, () -> client.withRetry(mockSupplier, "test"));
    verify(mockSupplier, times(4)).get();
    _metricUtils.verify(
        () -> MetricUtils.counter(client.getClass(), "test_exception_" + e.getClass().getName()),
        times(4));
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
    _metricUtils.verify(
        () -> MetricUtils.counter(client.getClass(), "exception_" + e.getClass().getName()),
        times(1));
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
                            .metadataChangeProposal(mcp)
                            .entitySpec(
                                opContext
                                    .getEntityRegistry()
                                    .getEntitySpec(Constants.CONTAINER_ENTITY_NAME))
                            .aspectSpec(
                                opContext
                                    .getEntityRegistry()
                                    .getEntitySpec(Constants.CONTAINER_ENTITY_NAME)
                                    .getAspectSpec(Constants.STATUS_ASPECT_NAME))
                            .auditStamp(auditStamp)
                            .build())
                    .result(UpdateAspectResult.builder().mcp(mcp).urn(testUrn).build())
                    .isUpdate(true)
                    .publishedMCL(true)
                    .sqlCommitted(true)
                    .build()));

    String urnStr = client.ingestProposal(opContext, mcp, false);

    assertEquals(urnStr, "urn:li:container:orderingTest");
  }
}
