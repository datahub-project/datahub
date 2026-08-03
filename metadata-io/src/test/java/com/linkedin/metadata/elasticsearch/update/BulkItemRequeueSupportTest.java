package com.linkedin.metadata.elasticsearch.update;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.graph.elastic.GraphEdgeWriteVersionFence;
import com.linkedin.metadata.search.elasticsearch.update.BulkItemRequeueSupport;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.testng.annotations.Test;

public class BulkItemRequeueSupportTest {

  @Test
  public void testRequeueUntilExhausted() {
    List<DocWriteRequest<?>> requeued = new ArrayList<>();
    Consumer<DocWriteRequest<?>> callback = requeued::add;
    BulkItemRequeueSupport support = new BulkItemRequeueSupport(true, 2, callback);

    IndexRequest request = mock(IndexRequest.class);
    when(request.index()).thenReturn("idx");
    when(request.id()).thenReturn("doc-1");

    assertEquals(support.tryRequeue(request), BulkItemRequeueSupport.Outcome.REQUEUED);
    assertEquals(support.tryRequeue(request), BulkItemRequeueSupport.Outcome.REQUEUED);
    assertEquals(support.tryRequeue(request), BulkItemRequeueSupport.Outcome.EXHAUSTED);
    assertEqualsSize(requeued, 2);
  }

  @Test
  public void testDisabled() {
    List<DocWriteRequest<?>> requeued = new ArrayList<>();
    BulkItemRequeueSupport support = new BulkItemRequeueSupport(false, 3, requeued::add);
    IndexRequest request = mock(IndexRequest.class);
    when(request.index()).thenReturn("idx");
    when(request.id()).thenReturn("doc-1");
    assertEquals(support.tryRequeue(request), BulkItemRequeueSupport.Outcome.DISABLED);
    assertEqualsSize(requeued, 0);
  }

  @Test
  public void testClearAttemptsAllowsRetry() {
    Consumer<DocWriteRequest<?>> callback = mock(Consumer.class);
    BulkItemRequeueSupport support = new BulkItemRequeueSupport(true, 1, callback);
    IndexRequest request = mock(IndexRequest.class);
    when(request.index()).thenReturn("idx");
    when(request.id()).thenReturn("doc-1");

    assertEquals(support.tryRequeue(request), BulkItemRequeueSupport.Outcome.REQUEUED);
    assertEquals(support.tryRequeue(request), BulkItemRequeueSupport.Outcome.EXHAUSTED);
    support.clearAttempts(request);
    assertEquals(support.tryRequeue(request), BulkItemRequeueSupport.Outcome.REQUEUED);
    verify(callback, times(2)).accept(request);
  }

  @Test
  public void testDeclineStaleGraphRequeueAfterNewerVersion() {
    synchronized (GraphEdgeWriteVersionFence.class) {
      GraphEdgeWriteVersionFence.INSTANCE.resetForTesting();
      List<DocWriteRequest<?>> requeued = new ArrayList<>();
      BulkItemRequeueSupport support = new BulkItemRequeueSupport(true, 3, requeued::add);

      String docId = "bulk-requeue-decline-stale";
      UpdateRequest staleUpsert = new UpdateRequest("graph_service_v1", docId);
      DeleteRequest newerDelete = new DeleteRequest("graph_service_v1").id(docId);
      GraphEdgeWriteVersionFence.INSTANCE.recordSubmit(docId, 2L, staleUpsert);
      GraphEdgeWriteVersionFence.INSTANCE.recordSubmit(docId, 3L, newerDelete);

      assertEquals(support.tryRequeue(staleUpsert), BulkItemRequeueSupport.Outcome.DECLINED_STALE);
      assertEquals(support.tryRequeue(newerDelete), BulkItemRequeueSupport.Outcome.REQUEUED);
      assertEqualsSize(requeued, 1);
    }
  }

  private static void assertEqualsSize(List<?> list, int expected) {
    if (list.size() != expected) {
      throw new AssertionError("Expected size " + expected + " but was " + list.size());
    }
  }
}
