package com.linkedin.metadata.search.elasticsearch.update;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;
import com.linkedin.metadata.utils.elasticsearch.BulkProcessorFlushResult;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.core.rest.RestStatus;
import org.testng.annotations.Test;

public class BulkProcessorCompletionTrackerTest {

  @Test
  public void testDocumentMissingRequestIsNotATransferFailure() {
    BulkProcessorCompletionTracker tracker = new BulkProcessorCompletionTracker();
    BulkProcessorCompletionTracker.Submission submission =
        tracker.track(new IndexRequest("datasetindex_v3").id("urn-missing"), ignored -> {});
    tracker.recordEs8Failure(
        List.of(submission), new IllegalStateException("document_missing_exception"));

    BulkProcessorFlushResult result = tracker.flushAndAwait(() -> {}, 1, TimeUnit.SECONDS);
    assertEquals(result.transportFailedOperations(), 0);
    assertEquals(result.unexpectedItemFailures(), 0);
  }

  @Test
  public void testCompletedTrafficIsAggregatedWithoutRetainingRequests() {
    BulkProcessorCompletionTracker tracker = new BulkProcessorCompletionTracker();

    for (int i = 0; i < 1000; i++) {
      BulkProcessorCompletionTracker.Submission submission =
          tracker.track(new IndexRequest("primaryindex_v3").id("urn-" + i), ignored -> {});
      tracker.recordEs8Failure(List.of(submission), new SocketTimeoutException("timed out"));
    }

    assertEquals(tracker.outstandingOperations(), 0);
    BulkProcessorFlushResult result = tracker.flushAndAwait(() -> {}, 1, TimeUnit.SECONDS);
    assertEquals(result.transportFailedOperations(), 1000);
  }

  @Test
  public void testDrainIgnoresForeignInFlightSubmissionAndPreservesItsOutcome() throws Exception {
    BulkProcessorCompletionTracker tracker = new BulkProcessorCompletionTracker();
    AtomicReference<BulkProcessorCompletionTracker.Submission> foreignSubmission =
        new AtomicReference<>();
    CountDownLatch submissionStarted = new CountDownLatch(1);
    CountDownLatch releaseSubmission = new CountDownLatch(1);

    // A foreign batch thread tracks a submission, blocks inside the native submit, then — once
    // released — completes it as a failure and drains on its own thread.
    CompletableFuture<BulkProcessorFlushResult> foreignBatch =
        CompletableFuture.supplyAsync(
            () -> {
              tracker.track(
                  new IndexRequest("primaryindex_v3").id("urn-1"),
                  submission -> {
                    foreignSubmission.set(submission);
                    submissionStarted.countDown();
                    try {
                      releaseSubmission.await();
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      throw new RuntimeException(e);
                    }
                  });
              tracker.recordEs8Failure(
                  List.of(foreignSubmission.get()), new SocketTimeoutException("timed out"));
              return tracker.flushAndAwait(() -> {}, 1, TimeUnit.SECONDS);
            });
    assertTrue(submissionStarted.await(1, TimeUnit.SECONDS));

    // A drain on this thread neither waits for nor counts the foreign in-flight submission.
    BulkProcessorFlushResult unrelatedDrain = tracker.flushAndAwait(() -> {}, 1, TimeUnit.SECONDS);
    assertEquals(unrelatedDrain.transportFailedOperations(), 0);
    assertEquals(unrelatedDrain.incompleteOperations(), 0);

    releaseSubmission.countDown();
    BulkProcessorFlushResult foreignResult = foreignBatch.get(2, TimeUnit.SECONDS);
    assertEquals(foreignResult.transportFailedOperations(), 1);
  }

  @Test
  public void testConcurrentBatchesReceiveTheirOwnFailures() throws Exception {
    BulkProcessorCompletionTracker tracker = new BulkProcessorCompletionTracker();
    CountDownLatch bothTracked = new CountDownLatch(2);
    CountDownLatch proceed = new CountDownLatch(1);
    ExecutorService batchThreads = Executors.newFixedThreadPool(2);

    // Two concurrent batches (e.g. two MAE listener threads) interleave track and drain on the
    // shared tracker; each fails its own document. Each drain must report exactly one failure —
    // before ownership scoping, whichever drain ran first captured both interleaved sequences
    // (two failures) and the other batch drained clean.
    try {
      CompletableFuture<BulkProcessorFlushResult> batchA =
          runFailingBatch(tracker, "urn-a", bothTracked, proceed, batchThreads);
      CompletableFuture<BulkProcessorFlushResult> batchB =
          runFailingBatch(tracker, "urn-b", bothTracked, proceed, batchThreads);

      assertTrue(bothTracked.await(1, TimeUnit.SECONDS));
      proceed.countDown();

      BulkProcessorFlushResult resultA = batchA.get(2, TimeUnit.SECONDS);
      BulkProcessorFlushResult resultB = batchB.get(2, TimeUnit.SECONDS);
      assertEquals(
          resultA.transportFailedOperations(), 1, "batch must see exactly its own failure");
      assertEquals(
          resultB.transportFailedOperations(), 1, "batch must see exactly its own failure");
      assertEquals(resultA.incompleteOperations(), 0);
      assertEquals(resultB.incompleteOperations(), 0);
    } finally {
      batchThreads.shutdownNow();
    }
  }

  private static CompletableFuture<BulkProcessorFlushResult> runFailingBatch(
      BulkProcessorCompletionTracker tracker,
      String docId,
      CountDownLatch tracked,
      CountDownLatch proceed,
      ExecutorService executor) {
    return CompletableFuture.supplyAsync(
        () -> {
          BulkProcessorCompletionTracker.Submission submission =
              tracker.track(new IndexRequest("primaryindex_v3").id(docId), ignored -> {});
          tracked.countDown();
          try {
            // Hold both batches open until each has tracked, so their sequences interleave
            // before either drains.
            assertTrue(proceed.await(1, TimeUnit.SECONDS));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
          tracker.recordEs8Failure(List.of(submission), new SocketTimeoutException("timed out"));
          return tracker.flushAndAwait(() -> {}, 1, TimeUnit.SECONDS);
        },
        executor);
  }

  // AI-862 item-level durability (ES8 client): a bulk item rejected with 429 (write-queue
  // backpressure) or 5xx is transient and must count as a retryable transport failure, not an
  // unexpected (poison) item failure — otherwise the drain commits past a silently-lost write. A
  // permanent 4xx stays poison; 409 stays a stale-version conflict; 2xx stays a successful write.
  @Test
  public void testItemLevelRetryableStatusesClassifiedRetryableForEs8Client() {
    BulkProcessorCompletionTracker tracker = new BulkProcessorCompletionTracker();
    List<Object> contexts =
        List.of(
            tracker.track(new IndexRequest("primaryindex_v3").id("urn-ok"), ignored -> {}),
            tracker.track(new IndexRequest("primaryindex_v3").id("urn-stale"), ignored -> {}),
            tracker.track(new IndexRequest("primaryindex_v3").id("urn-429"), ignored -> {}),
            tracker.track(new IndexRequest("primaryindex_v3").id("urn-503"), ignored -> {}),
            tracker.track(new IndexRequest("primaryindex_v3").id("urn-400"), ignored -> {}));
    co.elastic.clients.elasticsearch.core.BulkResponse response =
        co.elastic.clients.elasticsearch.core.BulkResponse.of(
            b ->
                b.errors(true)
                    .took(1)
                    .items(
                        es8Item("urn-ok", 201, null),
                        es8Item("urn-stale", 409, "version_conflict_engine_exception"),
                        es8Item("urn-429", 429, "es_rejected_execution_exception"),
                        es8Item("urn-503", 503, "unavailable_shards_exception"),
                        es8Item("urn-400", 400, "mapper_parsing_exception")));
    tracker.recordEs8Response(contexts, response);

    BulkProcessorFlushResult result = tracker.flushAndAwait(() -> {}, 1, TimeUnit.SECONDS);
    assertEquals(result.successfulV3Writes(), 1);
    assertEquals(result.staleVersionRejected(), 1);
    assertEquals(result.transportFailedOperations(), 2, "429 and 503 items must be retryable");
    assertEquals(result.unexpectedItemFailures(), 1, "only the 4xx item stays poison");
  }

  // AI-862 item-level durability (OpenSearch client): same classification contract as the ES8 path.
  @Test
  public void testItemLevelRetryableStatusesClassifiedRetryableForOpenSearchClient() {
    BulkProcessorCompletionTracker tracker = new BulkProcessorCompletionTracker();
    DocWriteRequest<?> ok = new IndexRequest("primaryindex_v3").id("urn-ok");
    DocWriteRequest<?> stale = new IndexRequest("primaryindex_v3").id("urn-stale");
    DocWriteRequest<?> rejected = new IndexRequest("primaryindex_v3").id("urn-429");
    DocWriteRequest<?> unavailable = new IndexRequest("primaryindex_v3").id("urn-503");
    DocWriteRequest<?> badRequest = new IndexRequest("primaryindex_v3").id("urn-400");
    for (DocWriteRequest<?> request : List.of(ok, stale, rejected, unavailable, badRequest)) {
      tracker.track(request, ignored -> {});
    }

    // Build the item mocks before stubbing response.getItems() — each openSearchItem() itself
    // stubs a mock, and nesting that inside when(...).thenReturn(...) trips Mockito's
    // unfinished-stubbing guard.
    BulkItemResponse[] items = {
      openSearchItem(201, false),
      openSearchItem(409, true),
      openSearchItem(429, true),
      openSearchItem(503, true),
      openSearchItem(400, true)
    };
    BulkRequest bulkRequest = mock(BulkRequest.class);
    when(bulkRequest.requests()).thenReturn(List.of(ok, stale, rejected, unavailable, badRequest));
    BulkResponse response = mock(BulkResponse.class);
    when(response.getItems()).thenReturn(items);
    tracker.recordOpenSearchResponse(bulkRequest, response);

    BulkProcessorFlushResult result = tracker.flushAndAwait(() -> {}, 1, TimeUnit.SECONDS);
    assertEquals(result.successfulV3Writes(), 1);
    assertEquals(result.staleVersionRejected(), 1);
    assertEquals(result.transportFailedOperations(), 2, "429 and 503 items must be retryable");
    assertEquals(result.unexpectedItemFailures(), 1, "only the 4xx item stays poison");
  }

  // A 429 counted on the draining thread must not be requeued from the callback thread: that new
  // submission belongs to the callback, so the drain already fails retryable and the requeue would
  // be a second write under the redelivery.
  @Test
  public void testDrainCountedRetryableRejectIsNotOwnedByTheCallbackThread() throws Exception {
    BulkProcessorCompletionTracker tracker = new BulkProcessorCompletionTracker();
    IndexRequest drained = new IndexRequest("datasetindex_v2").id("urn-429");
    IndexRequest callbackThread = new IndexRequest("datasetindex_v2").id("urn-callback");
    AtomicBoolean drainedWasCounted = new AtomicBoolean();
    AtomicBoolean callbackWasCounted = new AtomicBoolean();

    CompletableFuture<BulkProcessorFlushResult> drain =
        CompletableFuture.supplyAsync(
            () -> {
              tracker.track(drained, ignored -> {});
              return tracker.flushAndAwait(
                  () -> {
                    recordOpenSearchStatus(tracker, drained, 429);
                    tracker.track(callbackThread, ignored -> {});
                    recordOpenSearchStatus(tracker, callbackThread, 429);
                    drainedWasCounted.set(tracker.countedByDrain(drained));
                    callbackWasCounted.set(tracker.countedByDrain(callbackThread));
                  },
                  2,
                  TimeUnit.SECONDS);
            });

    BulkProcessorFlushResult result = drain.get(3, TimeUnit.SECONDS);
    assertEquals(result.transportFailedOperations(), 1);
    assertEquals(result.incompleteOperations(), 0);
    assertTrue(drainedWasCounted.get(), "the draining thread's 429 is marked so requeue skips it");
    assertFalse(
        callbackWasCounted.get(),
        "a submission created on another thread is not part of the drain");
    tracker.shutdownFlushExecutor();
  }

  // A drain that times out on a pending write fails retryable and the caller redelivers. The late
  // callback finds no submission, so the write must still read as counted by the drain or the
  // listener would requeue a second copy.
  @Test
  public void testWriteStillPendingWhenDrainTimesOutIsCountedByThatDrain() {
    BulkProcessorCompletionTracker tracker = new BulkProcessorCompletionTracker();
    IndexRequest pending = new IndexRequest("datasetindex_v2").id("urn-pending");
    tracker.track(pending, ignored -> {});

    BulkProcessorFlushResult result = tracker.flushAndAwait(() -> {}, 50, TimeUnit.MILLISECONDS);

    assertTrue(result.timedOut());
    assertEquals(result.incompleteOperations(), 1);
    assertTrue(tracker.countedByDrain(pending), "the late callback must not requeue this write");
    assertFalse(tracker.countedByDrain(pending), "the mark is taken once");
    tracker.shutdownFlushExecutor();
  }

  // AI-862 drain-slot contention: only one drain may hold the slot at a time. When a predecessor
  // drain never releases it, a racing drain must not block forever — beginDrain gives up after its
  // own budget and returns a not-acquired boundary, and the flush reports a retryable timeout
  // (counts stay zero, nothing is falsely committed) so the caller redelivers.
  @Test
  public void testDrainThatCannotClaimSlotWithinBudgetReportsTimeout() {
    BulkProcessorCompletionTracker tracker = new BulkProcessorCompletionTracker();

    // Claim the single drain slot and never release it.
    BulkProcessorCompletionTracker.DrainBoundary held = tracker.beginDrain(10, TimeUnit.SECONDS);
    assertNotNull(held);

    // A second flush cannot claim the slot within its short budget: it times out without touching
    // the predecessor's outcomes.
    BulkProcessorFlushResult result = tracker.flushAndAwait(() -> {}, 50, TimeUnit.MILLISECONDS);

    assertTrue(result.timedOut(), "a drain that never acquired the slot must report a timeout");
    assertEquals(result.successfulV3Writes(), 0);
    assertEquals(result.transportFailedOperations(), 0);
    assertEquals(result.incompleteOperations(), 0);
  }

  // AI-862 flush coalescing: the executor clears its in-progress flag only after the flush body
  // returns, so the flag briefly lags completion. A racing flush waits out that settle window; when
  // the in-flight flush is genuinely stuck (never returns), the racer fast-fails with a recorded
  // timeout instead of queueing behind it, so repeated drains cannot pile up on one stuck flush.
  @Test
  public void testSecondFlushAgainstStuckInFlightFlushReportsTimeoutInsteadOfQueueing()
      throws Exception {
    BulkProcessorCompletionTracker tracker = new BulkProcessorCompletionTracker();
    BulkProcessorCompletionTracker.DrainBoundary boundary =
        tracker.beginDrain(10, TimeUnit.SECONDS);
    CountDownLatch flushRunning = new CountDownLatch(1);
    CountDownLatch releaseFlush = new CountDownLatch(1);

    // First flush blocks inside the executor, so the in-progress flag stays set and its future
    // stays running.
    Future<?> stuckFlush =
        tracker.submitFlush(
            boundary,
            () -> {
              flushRunning.countDown();
              try {
                releaseFlush.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    try {
      assertNotNull(stuckFlush);
      assertTrue(flushRunning.await(1, TimeUnit.SECONDS));

      // A second flush submitted while the first is still in progress cannot claim the flag; it
      // waits the settle bound, sees the flush still stuck, and fast-fails with a null future.
      Future<?> racingFlush = tracker.submitFlush(boundary, () -> {});
      assertNull(racingFlush, "a racing flush must not queue behind a stuck in-flight flush");
      assertTrue(
          tracker.awaitDrain(boundary, 1, TimeUnit.SECONDS).timedOut(),
          "the racing flush must record a timeout on the drain");
    } finally {
      releaseFlush.countDown();
      stuckFlush.get(2, TimeUnit.SECONDS);
      tracker.shutdownFlushExecutor();
    }
  }

  // AI-862 flush coalescing under interrupt: when the settle wait for a stuck in-flight flush is
  // interrupted, the racing flush must not swallow the interrupt — it fast-fails (null, recording a
  // timeout) and restores the interrupt status so the caller can react.
  @Test
  public void testRacingFlushInterruptedWhileSettlingFastFailsAndRestoresInterrupt()
      throws Exception {
    BulkProcessorCompletionTracker tracker = new BulkProcessorCompletionTracker();
    BulkProcessorCompletionTracker.DrainBoundary boundary =
        tracker.beginDrain(10, TimeUnit.SECONDS);
    CountDownLatch flushRunning = new CountDownLatch(1);
    CountDownLatch releaseFlush = new CountDownLatch(1);
    Future<?> stuckFlush =
        tracker.submitFlush(
            boundary,
            () -> {
              flushRunning.countDown();
              try {
                releaseFlush.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    try {
      assertTrue(flushRunning.await(1, TimeUnit.SECONDS));

      AtomicBoolean interruptRestored = new AtomicBoolean(false);
      AtomicReference<Future<?>> racing = new AtomicReference<>();
      Thread racer =
          new Thread(
              () -> {
                // Pre-set the interrupt so the settle wait's blocking get() throws
                // InterruptedException deterministically, with no timing window.
                Thread.currentThread().interrupt();
                racing.set(tracker.submitFlush(boundary, () -> {}));
                interruptRestored.set(Thread.currentThread().isInterrupted());
              });
      racer.start();
      racer.join(2000);

      assertNull(racing.get(), "an interrupted racing flush must fast-fail instead of queueing");
      assertTrue(
          interruptRestored.get(), "the interrupted settle wait must restore the interrupt status");
    } finally {
      releaseFlush.countDown();
      stuckFlush.get(2, TimeUnit.SECONDS);
      tracker.shutdownFlushExecutor();
    }
  }

  // AI-862 drain-slot contention under interrupt: a drain waiting for the slot keeps the prior
  // uninterruptible semantics — an interrupt does not abort the wait and hand back a half-drained
  // result; the wait runs out its budget and the interrupt status is restored afterward so callers
  // up the stack still observe it.
  @Test
  public void testDrainWaitingForSlotRestoresInterruptStatus() throws Exception {
    BulkProcessorCompletionTracker tracker = new BulkProcessorCompletionTracker();
    tracker.beginDrain(10, TimeUnit.SECONDS); // hold the slot, never release it

    AtomicBoolean interruptRestored = new AtomicBoolean(false);
    AtomicReference<BulkProcessorFlushResult> result = new AtomicReference<>();
    CountDownLatch started = new CountDownLatch(1);
    Thread waiter =
        new Thread(
            () -> {
              started.countDown();
              result.set(tracker.flushAndAwait(() -> {}, 300, TimeUnit.MILLISECONDS));
              interruptRestored.set(Thread.currentThread().isInterrupted());
            });
    waiter.start();

    assertTrue(started.await(1, TimeUnit.SECONDS));
    Thread.sleep(50); // let the waiter enter the slot wait before interrupting it
    waiter.interrupt();
    waiter.join(2000);

    assertTrue(
        interruptRestored.get(),
        "the interrupt status must be restored after the drain gives up the slot");
    assertTrue(result.get().timedOut(), "the interrupted drain must still report a timeout");
  }

  private static BulkResponseItem es8Item(String id, int status, String errorType) {
    return BulkResponseItem.of(
        b -> {
          b.index("primaryindex_v3").id(id).operationType(OperationType.Index).status(status);
          return errorType == null
              ? b
              : b.error(ErrorCause.of(e -> e.type(errorType).reason(errorType)));
        });
  }

  private static void recordOpenSearchStatus(
      BulkProcessorCompletionTracker tracker, DocWriteRequest<?> request, int status) {
    BulkItemResponse item = openSearchItem(status, status >= 400);
    BulkRequest bulkRequest = mock(BulkRequest.class);
    when(bulkRequest.requests()).thenReturn(List.of(request));
    BulkResponse response = mock(BulkResponse.class);
    when(response.getItems()).thenReturn(new BulkItemResponse[] {item});
    tracker.recordOpenSearchResponse(bulkRequest, response);
  }

  private static BulkItemResponse openSearchItem(int status, boolean failed) {
    BulkItemResponse item = mock(BulkItemResponse.class);
    when(item.getIndex()).thenReturn("primaryindex_v3");
    when(item.getOpType()).thenReturn(DocWriteRequest.OpType.INDEX);
    when(item.status()).thenReturn(RestStatus.fromCode(status));
    when(item.isFailed()).thenReturn(failed);
    return item;
  }
}
