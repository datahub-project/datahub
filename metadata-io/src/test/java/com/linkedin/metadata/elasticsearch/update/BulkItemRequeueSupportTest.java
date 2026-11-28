package com.linkedin.metadata.elasticsearch.update;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.search.elasticsearch.update.BulkItemRequeueSupport;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.index.IndexRequest;
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

    assertTrue(support.tryRequeue(request));
    assertTrue(support.tryRequeue(request));
    assertFalse(support.tryRequeue(request));
    assertEqualsSize(requeued, 2);
  }

  @Test
  public void testDisabled() {
    List<DocWriteRequest<?>> requeued = new ArrayList<>();
    BulkItemRequeueSupport support = new BulkItemRequeueSupport(false, 3, requeued::add);
    IndexRequest request = mock(IndexRequest.class);
    when(request.index()).thenReturn("idx");
    when(request.id()).thenReturn("doc-1");
    assertFalse(support.tryRequeue(request));
    assertEqualsSize(requeued, 0);
  }

  @Test
  public void testClearAttemptsAllowsRetry() {
    Consumer<DocWriteRequest<?>> callback = mock(Consumer.class);
    BulkItemRequeueSupport support = new BulkItemRequeueSupport(true, 1, callback);
    IndexRequest request = mock(IndexRequest.class);
    when(request.index()).thenReturn("idx");
    when(request.id()).thenReturn("doc-1");

    assertTrue(support.tryRequeue(request));
    assertFalse(support.tryRequeue(request));
    support.clearAttempts(request);
    assertTrue(support.tryRequeue(request));
    verify(callback, times(2)).accept(request);
  }

  @Test
  public void testCallbackFailureDeclinesRequeueWithoutThrowing() {
    Consumer<DocWriteRequest<?>> callback = mock(Consumer.class);
    doThrow(new IllegalStateException("bulk process already closed"))
        .doNothing()
        .when(callback)
        .accept(org.mockito.ArgumentMatchers.any());
    BulkItemRequeueSupport support = new BulkItemRequeueSupport(true, 1, callback);
    IndexRequest request = mock(IndexRequest.class);
    when(request.index()).thenReturn("idx");
    when(request.id()).thenReturn("doc-1");

    assertFalse(support.tryRequeue(request));
    // The failed attempt is not charged against the item's budget.
    assertTrue(support.tryRequeue(request));
  }

  private static void assertEqualsSize(List<?> list, int expected) {
    if (list.size() != expected) {
      throw new AssertionError("Expected size " + expected + " but was " + list.size());
    }
  }
}
