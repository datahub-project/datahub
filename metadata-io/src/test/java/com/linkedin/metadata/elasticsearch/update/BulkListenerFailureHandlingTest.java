package com.linkedin.metadata.elasticsearch.update;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.graph.elastic.GraphEdgeWriteVersionFence;
import com.linkedin.metadata.search.elasticsearch.update.BulkItemRequeueSupport;
import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import com.linkedin.metadata.search.elasticsearch.update.BulkWriteResultTracker;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.time.Duration;
import java.util.Collections;
import java.util.function.Consumer;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.core.rest.RestStatus;
import org.testng.annotations.Test;

public class BulkListenerFailureHandlingTest {

  @Test
  public void testSuccessCompletesPending() {
    BulkWriteResultTracker tracker = new BulkWriteResultTracker();
    tracker.recordEnqueued(1);
    BulkListener listener =
        BulkListener.create(WriteRequest.RefreshPolicy.NONE, null, tracker, null);

    IndexRequest indexRequest = new IndexRequest("idx").id("1").source(Collections.emptyMap());
    BulkRequest bulkRequest = new BulkRequest().add(indexRequest);
    BulkItemResponse item = mock(BulkItemResponse.class);
    when(item.isFailed()).thenReturn(false);
    when(item.getOpType()).thenReturn(DocWriteRequest.OpType.INDEX);
    when(item.status()).thenReturn(RestStatus.OK);

    BulkResponse response = mock(BulkResponse.class);
    when(response.hasFailures()).thenReturn(false);
    when(response.getItems()).thenReturn(new BulkItemResponse[] {item});
    when(response.getIngestTookInMillis()).thenReturn(BulkResponse.NO_INGEST_TOOK);
    when(response.getTook()).thenReturn(org.opensearch.common.unit.TimeValue.timeValueMillis(1));

    listener.afterBulk(1L, bulkRequest, response);
    assertEquals(tracker.getPendingItems(), 0);
  }

  @Test
  public void testVersionConflictRequeue() {
    BulkWriteResultTracker tracker = new BulkWriteResultTracker();
    tracker.recordEnqueued(1);
    Consumer<DocWriteRequest<?>> requeue = mock(Consumer.class);
    BulkItemRequeueSupport support = new BulkItemRequeueSupport(true, 3, requeue);
    BulkListener listener =
        BulkListener.create(WriteRequest.RefreshPolicy.NONE, null, tracker, support);

    IndexRequest indexRequest = new IndexRequest("idx").id("1").source(Collections.emptyMap());
    BulkRequest bulkRequest = new BulkRequest().add(indexRequest);

    BulkItemResponse.Failure failure = mock(BulkItemResponse.Failure.class);
    when(failure.getStatus()).thenReturn(RestStatus.CONFLICT);
    when(failure.getMessage()).thenReturn("version_conflict_engine_exception");

    BulkItemResponse item = mock(BulkItemResponse.class);
    when(item.isFailed()).thenReturn(true);
    when(item.getFailure()).thenReturn(failure);
    when(item.getFailureMessage()).thenReturn("version_conflict_engine_exception");
    when(item.getOpType()).thenReturn(DocWriteRequest.OpType.INDEX);
    when(item.status()).thenReturn(RestStatus.CONFLICT);

    BulkResponse response = mock(BulkResponse.class);
    when(response.hasFailures()).thenReturn(true);
    when(response.getItems()).thenReturn(new BulkItemResponse[] {item});
    when(response.getIngestTookInMillis()).thenReturn(BulkResponse.NO_INGEST_TOOK);
    when(response.getTook()).thenReturn(org.opensearch.common.unit.TimeValue.timeValueMillis(1));
    when(response.buildFailureMessage()).thenReturn("conflict");

    listener.afterBulk(1L, bulkRequest, response);

    verify(requeue, times(1)).accept(any(DocWriteRequest.class));
    // still pending while requeued
    assertEquals(tracker.getPendingItems(), 1);
    assertEquals(tracker.getUnrecoveredTransferFailures(), 0);
  }

  @Test
  public void testExhaustedConflictIsLwwNotTransferFailure() {
    BulkWriteResultTracker tracker = new BulkWriteResultTracker();
    tracker.recordEnqueued(1);
    // maxAttempts 0 disables requeue
    BulkItemRequeueSupport support = new BulkItemRequeueSupport(true, 0, req -> {});
    BulkListener listener =
        BulkListener.create(WriteRequest.RefreshPolicy.NONE, null, tracker, support);

    IndexRequest indexRequest = new IndexRequest("idx").id("1").source(Collections.emptyMap());
    BulkRequest bulkRequest = new BulkRequest().add(indexRequest);

    BulkItemResponse.Failure failure = mock(BulkItemResponse.Failure.class);
    when(failure.getStatus()).thenReturn(RestStatus.CONFLICT);
    when(failure.getMessage()).thenReturn("version_conflict_engine_exception");

    BulkItemResponse item = mock(BulkItemResponse.class);
    when(item.isFailed()).thenReturn(true);
    when(item.getFailure()).thenReturn(failure);
    when(item.getFailureMessage()).thenReturn("version_conflict_engine_exception");
    when(item.getOpType()).thenReturn(DocWriteRequest.OpType.INDEX);
    when(item.status()).thenReturn(RestStatus.CONFLICT);

    BulkResponse response = mock(BulkResponse.class);
    when(response.hasFailures()).thenReturn(true);
    when(response.getItems()).thenReturn(new BulkItemResponse[] {item});
    when(response.getIngestTookInMillis()).thenReturn(BulkResponse.NO_INGEST_TOOK);
    when(response.getTook()).thenReturn(org.opensearch.common.unit.TimeValue.timeValueMillis(1));
    when(response.buildFailureMessage()).thenReturn("conflict");

    listener.afterBulk(1L, bulkRequest, response);
    assertEquals(tracker.getPendingItems(), 0);
    assertEquals(tracker.getUnrecoveredTransferFailures(), 0);
  }

  @Test
  public void testNonRetriableIsTransferFailure() {
    BulkWriteResultTracker tracker = new BulkWriteResultTracker();
    tracker.recordEnqueued(1);
    Consumer<DocWriteRequest<?>> requeue = mock(Consumer.class);
    BulkItemRequeueSupport support = new BulkItemRequeueSupport(true, 3, requeue);
    BulkListener listener =
        BulkListener.create(WriteRequest.RefreshPolicy.NONE, null, tracker, support);

    IndexRequest indexRequest = new IndexRequest("idx").id("1").source(Collections.emptyMap());
    BulkRequest bulkRequest = new BulkRequest().add(indexRequest);

    BulkItemResponse.Failure failure = mock(BulkItemResponse.Failure.class);
    when(failure.getStatus()).thenReturn(RestStatus.BAD_REQUEST);
    when(failure.getMessage()).thenReturn("mapper_parsing_exception");

    BulkItemResponse item = mock(BulkItemResponse.class);
    when(item.isFailed()).thenReturn(true);
    when(item.getFailure()).thenReturn(failure);
    when(item.getFailureMessage()).thenReturn("mapper_parsing_exception");
    when(item.getOpType()).thenReturn(DocWriteRequest.OpType.INDEX);
    when(item.status()).thenReturn(RestStatus.BAD_REQUEST);

    BulkResponse response = mock(BulkResponse.class);
    when(response.hasFailures()).thenReturn(true);
    when(response.getItems()).thenReturn(new BulkItemResponse[] {item});
    when(response.getIngestTookInMillis()).thenReturn(BulkResponse.NO_INGEST_TOOK);
    when(response.getTook()).thenReturn(org.opensearch.common.unit.TimeValue.timeValueMillis(1));
    when(response.buildFailureMessage()).thenReturn("mapper");

    listener.afterBulk(1L, bulkRequest, response);
    verify(requeue, never()).accept(any());
    assertEquals(tracker.getPendingItems(), 0);
    assertEquals(tracker.getUnrecoveredTransferFailures(), 1);
  }

  @Test
  public void testDeclinedStaleOnRetriableStatusIsCompletedNotUnrecovered() {
    synchronized (GraphEdgeWriteVersionFence.class) {
      GraphEdgeWriteVersionFence.INSTANCE.resetForTesting();
      BulkWriteResultTracker tracker = new BulkWriteResultTracker();
      tracker.recordEnqueued(1);
      Consumer<DocWriteRequest<?>> requeue = mock(Consumer.class);
      BulkItemRequeueSupport support = new BulkItemRequeueSupport(true, 3, requeue);
      BulkListener listener =
          BulkListener.create(WriteRequest.RefreshPolicy.NONE, null, tracker, support);

      String docId = "bulk-listener-declined-stale-429";
      UpdateRequest staleUpsert = new UpdateRequest("graph_service_v1", docId);
      DeleteRequest newerDelete = new DeleteRequest("graph_service_v1").id(docId);
      GraphEdgeWriteVersionFence.INSTANCE.recordSubmit(docId, 2L, staleUpsert);
      GraphEdgeWriteVersionFence.INSTANCE.recordSubmit(docId, 3L, newerDelete);

      BulkRequest bulkRequest = new BulkRequest().add(staleUpsert);

      BulkItemResponse.Failure failure = mock(BulkItemResponse.Failure.class);
      when(failure.getStatus()).thenReturn(RestStatus.TOO_MANY_REQUESTS);
      when(failure.getMessage()).thenReturn("rejected_execution_exception");

      BulkItemResponse item = mock(BulkItemResponse.class);
      when(item.isFailed()).thenReturn(true);
      when(item.getFailure()).thenReturn(failure);
      when(item.getFailureMessage()).thenReturn("rejected_execution_exception");
      when(item.getOpType()).thenReturn(DocWriteRequest.OpType.UPDATE);
      when(item.status()).thenReturn(RestStatus.TOO_MANY_REQUESTS);
      when(item.getIndex()).thenReturn("graph_service_v1");
      when(item.getId()).thenReturn(docId);

      BulkResponse response = mock(BulkResponse.class);
      when(response.hasFailures()).thenReturn(true);
      when(response.getItems()).thenReturn(new BulkItemResponse[] {item});
      when(response.getIngestTookInMillis()).thenReturn(BulkResponse.NO_INGEST_TOOK);
      when(response.getTook()).thenReturn(org.opensearch.common.unit.TimeValue.timeValueMillis(1));
      when(response.buildFailureMessage()).thenReturn("429");

      listener.afterBulk(1L, bulkRequest, response);

      verify(requeue, never()).accept(any());
      assertTrue(tracker.isIdle());
      assertEquals(tracker.getUnrecoveredTransferFailures(), 0);
      assertEquals(tracker.drainUnrecoveredTransferFailures(), 0);
    }
  }

  /**
   * Declining a superseded graph requeue must not surface as unrecovered transfer failure under
   * ack-after-transfer: {@link ESBulkProcessor#flushAndWait} would otherwise throw and block offset
   * ack.
   */
  @Test
  public void testDeclinedStaleDoesNotBreakAckAfterTransferFlushAndWait() throws Exception {
    synchronized (GraphEdgeWriteVersionFence.class) {
      GraphEdgeWriteVersionFence.INSTANCE.resetForTesting();
      BulkWriteResultTracker tracker = new BulkWriteResultTracker();
      tracker.recordEnqueued(1);
      BulkItemRequeueSupport support = new BulkItemRequeueSupport(true, 3, req -> {});
      BulkListener listener =
          BulkListener.create(WriteRequest.RefreshPolicy.NONE, null, tracker, support);

      String docId = "bulk-listener-declined-stale-ack";
      UpdateRequest staleUpsert = new UpdateRequest("graph_service_v1", docId);
      DeleteRequest newerDelete = new DeleteRequest("graph_service_v1").id(docId);
      GraphEdgeWriteVersionFence.INSTANCE.recordSubmit(docId, 2L, staleUpsert);
      GraphEdgeWriteVersionFence.INSTANCE.recordSubmit(docId, 3L, newerDelete);

      BulkItemResponse.Failure failure = mock(BulkItemResponse.Failure.class);
      when(failure.getStatus()).thenReturn(RestStatus.TOO_MANY_REQUESTS);
      when(failure.getMessage()).thenReturn("rejected_execution_exception");

      BulkItemResponse item = mock(BulkItemResponse.class);
      when(item.isFailed()).thenReturn(true);
      when(item.getFailure()).thenReturn(failure);
      when(item.getFailureMessage()).thenReturn("rejected_execution_exception");
      when(item.getOpType()).thenReturn(DocWriteRequest.OpType.UPDATE);
      when(item.status()).thenReturn(RestStatus.TOO_MANY_REQUESTS);

      BulkResponse response = mock(BulkResponse.class);
      when(response.hasFailures()).thenReturn(true);
      when(response.getItems()).thenReturn(new BulkItemResponse[] {item});
      when(response.getIngestTookInMillis()).thenReturn(BulkResponse.NO_INGEST_TOOK);
      when(response.getTook()).thenReturn(org.opensearch.common.unit.TimeValue.timeValueMillis(1));
      when(response.buildFailureMessage()).thenReturn("429");

      listener.afterBulk(1L, new BulkRequest().add(staleUpsert), response);

      assertTrue(tracker.isIdle());
      assertEquals(tracker.getUnrecoveredTransferFailures(), 0);

      SearchClientShim<?> searchClient = mock(SearchClientShim.class);
      doNothing()
          .when(searchClient)
          .generateBulkProcessor(any(), any(), anyInt(), anyLong(), anyLong(), anyInt(), anyInt());
      doNothing()
          .when(searchClient)
          .configureBulkProcessorWriteOptions(any(Boolean.class), anyInt());
      doNothing().when(searchClient).flushBulkProcessor();
      doAnswer(
              inv -> {
                tracker.awaitIdle(Duration.ofMillis(inv.getArgument(0)));
                return null;
              })
          .when(searchClient)
          .flushAndAwaitBulkTransfer(anyLong());
      when(searchClient.drainBulkTransferFailures())
          .thenAnswer(inv -> tracker.drainUnrecoveredTransferFailures());

      ESBulkProcessor processor =
          ESBulkProcessor.builder(searchClient, null)
              .ackAfterTransfer(true)
              .ackAfterTransferTimeoutSeconds(5)
              .build();

      // Would throw BulkTransferException if declined-stale were counted as unrecovered
      processor.flushAndWait(Duration.ofSeconds(5));
      assertTrue(tracker.isIdle());
    }
  }

  @Test
  public void testDeclinedStaleOnTransportFailureIsCompletedNotUnrecovered() {
    synchronized (GraphEdgeWriteVersionFence.class) {
      GraphEdgeWriteVersionFence.INSTANCE.resetForTesting();
      BulkWriteResultTracker tracker = new BulkWriteResultTracker();
      tracker.recordEnqueued(1);
      Consumer<DocWriteRequest<?>> requeue = mock(Consumer.class);
      BulkItemRequeueSupport support = new BulkItemRequeueSupport(true, 3, requeue);
      BulkListener listener =
          BulkListener.create(WriteRequest.RefreshPolicy.NONE, null, tracker, support);

      String docId = "bulk-listener-declined-stale-transport";
      UpdateRequest staleUpsert = new UpdateRequest("graph_service_v1", docId);
      DeleteRequest newerDelete = new DeleteRequest("graph_service_v1").id(docId);
      GraphEdgeWriteVersionFence.INSTANCE.recordSubmit(docId, 2L, staleUpsert);
      GraphEdgeWriteVersionFence.INSTANCE.recordSubmit(docId, 3L, newerDelete);

      BulkRequest bulkRequest = new BulkRequest().add(staleUpsert);

      listener.afterBulk(1L, bulkRequest, new RuntimeException("transport"));

      verify(requeue, never()).accept(any());
      assertEquals(tracker.getPendingItems(), 0);
      assertEquals(tracker.drainUnrecoveredTransferFailures(), 0);
    }
  }
}
