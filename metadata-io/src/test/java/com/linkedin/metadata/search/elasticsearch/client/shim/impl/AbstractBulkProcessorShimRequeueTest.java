package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static com.linkedin.metadata.search.elasticsearch.client.shim.impl.BlockingTestBulkProcessorShim.CEILING_MILLIS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.opensearch.action.index.IndexRequest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Requeue runs off the bulk callback thread and keeps unrecovered accounting exact. */
public class AbstractBulkProcessorShimRequeueTest {

  private BlockingTestBulkProcessorShim shim;

  @BeforeMethod
  public void resetState() {
    shim = null;
  }

  @AfterMethod
  public void releaseBlockedThreads() {
    if (shim != null) {
      shim.releaseBlocked();
    }
  }

  @Test
  public void testRequeueReturnsWithoutWaitingForProcessorAdd() throws Exception {
    shim = new BlockingTestBulkProcessorShim();
    shim.blockAdd = true;
    shim.getBulkWriteResultTracker().recordEnqueued(1);

    long started = System.nanoTime();
    shim.requeueFailedRequest(new IndexRequest("idx").id("doc-1"));

    assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < CEILING_MILLIS);
    assertTrue(shim.addStarted.await(CEILING_MILLIS, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testFailedRequeueIsRecordedAsUnrecoveredFailure() throws Exception {
    shim = new BlockingTestBulkProcessorShim();
    shim.failAdd = true;
    shim.getBulkWriteResultTracker().recordEnqueued(1);

    shim.requeueFailedRequest(new IndexRequest("idx").id("doc-1"));

    // The failed re-add resolves the item that was pending from the original add.
    shim.getBulkWriteResultTracker().awaitIdle(Duration.ofMillis(CEILING_MILLIS));
    assertEquals(shim.getBulkWriteResultTracker().drainUnrecoveredTransferFailures(), 1L);
  }

  @Test
  public void testErrorDuringRequeueIsRecordedAndWorkerKeepsRunning() throws Exception {
    shim = new BlockingTestBulkProcessorShim();
    shim.failAddWithError = true;
    shim.getBulkWriteResultTracker().recordEnqueued(2);

    shim.requeueFailedRequest(new IndexRequest("idx").id("doc-1"));
    assertTrue(shim.errorThrown.await(CEILING_MILLIS, TimeUnit.MILLISECONDS));
    // The pool replaces the worker that died, so a later requeue is still processed.
    shim.failAddWithError = false;
    shim.failAdd = true;
    shim.requeueFailedRequest(new IndexRequest("idx").id("doc-2"));

    shim.getBulkWriteResultTracker().awaitIdle(Duration.ofMillis(CEILING_MILLIS));
    assertEquals(shim.getBulkWriteResultTracker().drainUnrecoveredTransferFailures(), 2L);
  }

  @Test
  public void testRequeueDeclinedWhenQueueIsFull() {
    shim = new BlockingTestBulkProcessorShim();
    shim.blockAdd = true;
    assertThrows(
        IllegalStateException.class,
        () -> {
          // The first re-add blocks on the worker, so the rest fill the queue until it is full.
          for (int i = 0; i < 5_000; i++) {
            shim.requeueFailedRequest(new IndexRequest("idx").id("doc-" + i));
          }
        });
  }

  @Test
  public void testQueuedRequeuesAreRecordedAsUnrecoveredWhenClosing() throws Exception {
    shim = new BlockingTestBulkProcessorShim();
    shim.blockAdd = true;
    shim.getBulkWriteResultTracker().recordEnqueued(3);
    shim.requeueFailedRequest(new IndexRequest("idx").id("doc-1"));
    assertTrue(shim.addStarted.await(CEILING_MILLIS, TimeUnit.MILLISECONDS));
    shim.requeueFailedRequest(new IndexRequest("idx").id("doc-2"));
    shim.requeueFailedRequest(new IndexRequest("idx").id("doc-3"));

    // The first item stays on the worker, so the drain times out and shutdown drops the queue.
    assertThrows(TimeoutException.class, () -> shim.closeBulkProcessorWithin(200));

    assertEquals(shim.getBulkWriteResultTracker().drainUnrecoveredTransferFailures(), 2L);
  }
}
