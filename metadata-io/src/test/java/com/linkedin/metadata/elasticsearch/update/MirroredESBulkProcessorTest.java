package com.linkedin.metadata.elasticsearch.update;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.search.elasticsearch.update.BulkTransferException;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.MigrationIndexNames;
import com.linkedin.metadata.search.elasticsearch.update.MirroredESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.MirroredWriteLog;
import com.linkedin.metadata.utils.elasticsearch.ConfiguredIndexPrefixResolver;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.script.Script;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MirroredESBulkProcessorTest {
  private static final String SERVING_INDEX = "prod_graph_service_v1";
  private static final String TARGET_INDEX = "tgt_graph_service_v1";
  private static final String LOG_INDEX = "tgt_cluster_migration_log_graph";

  private SearchClientShim<?> servingShim;
  private SearchClientShim<?> targetShim;
  private ESBulkProcessor serving;
  private ESBulkProcessor target;
  private MigrationIndexNames names;
  private OperationContext opContext;

  @BeforeMethod
  public void setUp() {
    servingShim = quietShim();
    targetShim = quietShim();
    serving = ESBulkProcessor.builder(servingShim, null).build();
    target = ESBulkProcessor.builder(targetShim, null).build();
    IndexConvention convention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder().hashIdAlgo("MD5").build(),
            new ConfiguredIndexPrefixResolver("prod"),
            new EntityIndexConfiguration(),
            Map.of());
    names = MigrationIndexNames.forFamily(convention, SearchComponent.GRAPH, "tgt");
    opContext = mock(OperationContext.class);
  }

  private static SearchClientShim<?> quietShim() {
    SearchClientShim<?> shim = mock(SearchClientShim.class);
    doNothing()
        .when(shim)
        .generateBulkProcessor(any(), any(), anyInt(), anyLong(), anyLong(), anyInt(), anyInt());
    doNothing().when(shim).configureBulkProcessorWriteOptions(any(Boolean.class), anyInt());
    doNothing().when(shim).flushBulkProcessor();
    return shim;
  }

  private MirroredESBulkProcessor mirrored(boolean withLog) {
    return new MirroredESBulkProcessor(
        serving, target, names, withLog ? new MirroredWriteLog(target, names) : null, null);
  }

  @Test
  public void testUpsertIsAppliedToBothClustersAsSeparateRequests() {
    UpdateRequest original =
        new UpdateRequest(SERVING_INDEX, "edge-1")
            .docAsUpsert(true)
            .doc("{\"a\":1}", XContentType.JSON);

    mirrored(true).add(opContext, "urn:li:dataset:a", original);

    verify(servingShim).addBulk(opContext, "urn:li:dataset:a", original);
    ArgumentCaptor<DocWriteRequest<?>> captor = ArgumentCaptor.forClass(DocWriteRequest.class);
    verify(targetShim).addBulk(eq(opContext), eq("urn:li:dataset:a"), captor.capture());
    UpdateRequest copy = (UpdateRequest) captor.getValue();
    assertNotSame(copy, original);
    assertEquals(original.index(), SERVING_INDEX);
    assertEquals(copy.index(), TARGET_INDEX);
    assertEquals(copy.id(), "edge-1");
    assertTrue(copy.docAsUpsert());
  }

  @Test
  public void testDeleteByIdIsMirroredAndLogged() {
    mirrored(true).add(opContext, "edge-2", new DeleteRequest(SERVING_INDEX, "edge-2"));

    ArgumentCaptor<DocWriteRequest<?>> captor = ArgumentCaptor.forClass(DocWriteRequest.class);
    verify(targetShim, times(2)).addBulk(eq(opContext), any(), captor.capture());
    List<DocWriteRequest<?>> targetWrites = captor.getAllValues();
    DeleteRequest delete = (DeleteRequest) targetWrites.get(0);
    assertEquals(delete.index(), TARGET_INDEX);
    assertEquals(delete.id(), "edge-2");
    IndexRequest logEntry = (IndexRequest) targetWrites.get(1);
    assertEquals(logEntry.index(), LOG_INDEX);
    Map<String, Object> entry = logEntry.sourceAsMap();
    assertEquals(entry.get(MirroredWriteLog.FIELD_KIND), "DELETE_BY_ID");
    assertEquals(entry.get(MirroredWriteLog.FIELD_INDEX), TARGET_INDEX);
    assertEquals(entry.get(MirroredWriteLog.FIELD_ID), "edge-2");
  }

  @Test
  public void testDeleteByIdWithoutALogIsOnlyMirrored() {
    mirrored(false).add(opContext, "edge-3", new DeleteRequest(SERVING_INDEX, "edge-3"));

    verify(targetShim, times(1)).addBulk(eq(opContext), any(), any());
  }

  @Test
  public void testASwitchedOffLogRecordsNothing() {
    MirroredWriteLog log = new MirroredWriteLog(target, names);
    log.setActive(false);

    new MirroredESBulkProcessor(serving, target, names, log, null)
        .add(opContext, "edge-4", new DeleteRequest(SERVING_INDEX, "edge-4"));

    verify(targetShim, times(1)).addBulk(eq(opContext), any(), any());
  }

  @Test
  public void testDeleteByQueryRunsOnBothClustersAndReturnsTheServingResult() throws Exception {
    BulkByScrollResponse servingResponse = mock(BulkByScrollResponse.class);
    when(servingShim.deleteByQuery(any(), any(), any())).thenReturn(servingResponse);
    when(targetShim.deleteByQuery(any(), any(), any()))
        .thenReturn(mock(BulkByScrollResponse.class));

    var response =
        mirrored(true)
            .deleteByQuery(
                opContext, QueryBuilders.termQuery("relationshipType", "x"), SERVING_INDEX);

    assertSame(response.orElseThrow(), servingResponse);
    ArgumentCaptor<DeleteByQueryRequest> servingCaptor =
        ArgumentCaptor.forClass(DeleteByQueryRequest.class);
    verify(servingShim).deleteByQuery(eq(opContext), servingCaptor.capture(), any());
    assertEquals(servingCaptor.getValue().indices(), new String[] {SERVING_INDEX});
    ArgumentCaptor<DeleteByQueryRequest> targetCaptor =
        ArgumentCaptor.forClass(DeleteByQueryRequest.class);
    verify(targetShim).deleteByQuery(eq(opContext), targetCaptor.capture(), any());
    assertEquals(targetCaptor.getValue().indices(), new String[] {TARGET_INDEX});

    ArgumentCaptor<DocWriteRequest<?>> logCaptor = ArgumentCaptor.forClass(DocWriteRequest.class);
    verify(targetShim).addBulk(eq(opContext), eq(TARGET_INDEX), logCaptor.capture());
    Map<String, Object> entry = ((IndexRequest) logCaptor.getValue()).sourceAsMap();
    assertEquals(entry.get(MirroredWriteLog.FIELD_KIND), "DELETE_BY_QUERY");
    assertTrue(entry.get(MirroredWriteLog.FIELD_QUERY).toString().contains("relationshipType"));
  }

  @Test
  public void testAByQueryTheServingClusterRefusedIsNeitherMirroredNorLogged() throws Exception {
    // The serving processor logs the failure and answers empty; the serving cluster still holds
    // what the query matched, so the target must keep it too, and no replay may take it away.
    when(servingShim.deleteByQuery(any(), any(), any()))
        .thenThrow(new RuntimeException("search phase failed"));

    var response =
        mirrored(true)
            .deleteByQuery(
                opContext, QueryBuilders.termQuery("relationshipType", "x"), SERVING_INDEX);

    assertTrue(response.isEmpty());
    verify(targetShim, never()).deleteByQuery(any(), any(), any());
    verify(targetShim, never()).addBulk(any(), any(), any());
  }

  @Test
  public void testUpdateByQueryIsMirroredAndLoggedByItsQuery() throws Exception {
    when(servingShim.updateByQuery(any(), any(), any()))
        .thenReturn(mock(BulkByScrollResponse.class));
    when(targetShim.updateByQuery(any(), any(), any()))
        .thenReturn(mock(BulkByScrollResponse.class));
    Script script = new Script("ctx._source.removed = true");

    mirrored(true).updateByQuery(opContext, script, QueryBuilders.matchAllQuery(), SERVING_INDEX);

    ArgumentCaptor<UpdateByQueryRequest> targetCaptor =
        ArgumentCaptor.forClass(UpdateByQueryRequest.class);
    verify(targetShim).updateByQuery(eq(opContext), targetCaptor.capture(), any());
    assertEquals(targetCaptor.getValue().indices(), new String[] {TARGET_INDEX});
    ArgumentCaptor<DocWriteRequest<?>> logCaptor = ArgumentCaptor.forClass(DocWriteRequest.class);
    verify(targetShim).addBulk(eq(opContext), eq(TARGET_INDEX), logCaptor.capture());
    Map<String, Object> entry = ((IndexRequest) logCaptor.getValue()).sourceAsMap();
    assertEquals(entry.get(MirroredWriteLog.FIELD_KIND), "UPDATE_BY_QUERY");
    assertTrue(entry.get(MirroredWriteLog.FIELD_QUERY).toString().contains("match_all"));
    // The replay rewrites the matched documents from the source, so the script is not kept.
    assertEquals(entry.keySet(), Set.of("ts", "kind", "index", "query"));
  }

  @Test
  public void testFlushAndWaitReportsFailuresFromBothClusters() throws Exception {
    when(servingShim.drainBulkTransferFailures()).thenReturn(2L);
    when(targetShim.drainBulkTransferFailures()).thenReturn(3L);

    try {
      mirrored(true).flushAndWait(Duration.ofSeconds(1));
      fail("expected the combined failures to surface");
    } catch (BulkTransferException e) {
      assertEquals(e.getFailureCount(), 5L);
    }
    verify(servingShim).flushAndAwaitBulkTransfer(1000L);
    verify(targetShim).flushAndAwaitBulkTransfer(1000L);
  }

  @Test
  public void testCloseFlushesBothButClosesNeither() throws Exception {
    mirrored(true).close();

    verify(servingShim).flushBulkProcessor();
    verify(targetShim).flushBulkProcessor();
    verify(servingShim, never()).closeBulkProcessor();
    verify(targetShim, never()).closeBulkProcessor();
  }

  @Test
  public void testMirroringOntoTheServingProcessorIsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new MirroredESBulkProcessor(serving, serving, names, null, null));
  }
}
