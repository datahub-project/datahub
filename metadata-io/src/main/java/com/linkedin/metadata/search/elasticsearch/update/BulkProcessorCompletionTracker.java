package com.linkedin.metadata.search.elasticsearch.update;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.linkedin.metadata.service.SearchIndexFailureClassifier;
import com.linkedin.metadata.utils.elasticsearch.BulkProcessorFlushResult;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.WeakHashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.index.engine.DocumentMissingException;

/**
 * Tracks completion callbacks for one native bulk processor. Sequence assignment and drain boundary
 * capture share a lock, while native submission and flush calls run outside it.
 *
 * <p>Outcomes are attributed to the thread that submitted them: a batch is tracked and drained on
 * one thread (the MAE listener thread or the LoadIndices step thread), so a drain waits for and
 * consumes only its own thread's submissions. Concurrent batches on other listener threads — and
 * synchronous GMS writes that never drain — keep their outcomes in per-thread buckets instead of
 * leaking into whichever drain happens to be active, which would return one batch's failure to
 * another and let the failing batch report success.
 */
public final class BulkProcessorCompletionTracker {
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition completionChanged = lock.newCondition();
  private final NavigableMap<Long, Submission> submissions = new TreeMap<>();
  private final Map<DocWriteRequest<?>, Submission> openSearchSubmissions = new IdentityHashMap<>();
  private final Map<Thread, OutcomeCounts> undrainedOutcomesByThread = new WeakHashMap<>();
  // Writes whose failure was already counted on the active drain. The bulk callback requeues on
  // its own thread, which this drain does not wait for, so a requeue and the caller's redelivery
  // would both hit the cluster. The listener checks this before requeue; the drain may already
  // have finished by then, so the mark is taken while the lock is still held in complete().
  private final Set<DocWriteRequest<?>> drainCountedRequests =
      Collections.newSetFromMap(new IdentityHashMap<>());
  private static final int MAX_DRAIN_COUNTED_REQUESTS = 1024;
  private final ThreadPoolExecutor flushExecutor;
  private final AtomicBoolean flushInProgress = new AtomicBoolean();
  // The flush task submitted for the current in-progress flush. A racing drain awaits this real
  // completion instead of recording a spurious timeout when it observes flushInProgress still set —
  // the executor clears that flag in its finally only after the flush body returns. Written and
  // read
  // only by draining threads, which are serialized by activeDrain; volatile for cross-thread reads.
  private volatile Future<?> inFlightFlush;
  private DrainBoundary activeDrain;
  private long nextSequence;

  // Upper bound a racing drain waits for an already-running flush to settle before treating it as
  // genuinely stuck and recording a timeout. Small enough that repeated drains against a stuck
  // flush
  // do not queue behind it, yet far larger than the microsecond-scale window between a flush body
  // returning and the executor clearing flushInProgress.
  // ponytail: fixed settle bound; revisit as a fraction of the drain budget if flush latencies
  // grow.
  private static final long FLUSH_SETTLE_BOUND_NANOS = TimeUnit.MILLISECONDS.toNanos(20);

  public BulkProcessorCompletionTracker() {
    flushExecutor =
        new ThreadPoolExecutor(
            1,
            1,
            1,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(1),
            runnable -> {
              Thread thread = new Thread(runnable, "bulk-processor-tracker-flush");
              thread.setDaemon(true);
              return thread;
            },
            new ThreadPoolExecutor.AbortPolicy());
    flushExecutor.allowCoreThreadTimeOut(true);
  }

  @Nonnull
  public Submission track(
      @Nonnull final DocWriteRequest<?> request, @Nonnull final Consumer<Submission> submitAction) {
    lock.lock();
    Submission submission;
    try {
      submission = new Submission(this, ++nextSequence, request);
      submissions.put(submission.sequence, submission);
      openSearchSubmissions.put(request, submission);
    } finally {
      lock.unlock();
    }

    try {
      submitAction.accept(submission);
    } catch (RuntimeException | Error e) {
      lock.lock();
      try {
        if (submission.outcome == Outcome.PENDING) {
          discard(submission);
        }
        submission.submissionInProgress = false;
        completionChanged.signalAll();
      } finally {
        lock.unlock();
      }
      throw e;
    }

    lock.lock();
    try {
      submission.submissionInProgress = false;
      completionChanged.signalAll();
      return submission;
    } finally {
      lock.unlock();
    }
  }

  @Nonnull
  public BulkProcessorFlushResult flushAndAwait(
      @Nonnull final Runnable flushAction, final long timeout, @Nonnull final TimeUnit timeUnit) {
    long timeoutNanos = Math.max(0, timeUnit.toNanos(timeout));
    long startNanos = System.nanoTime();
    DrainBoundary drainBoundary =
        beginDrain(remainingNanos(startNanos, timeoutNanos), TimeUnit.NANOSECONDS);
    try {
      boolean readyToFlush =
          awaitSubmissions(
              drainBoundary, remainingNanos(startNanos, timeoutNanos), TimeUnit.NANOSECONDS);
      Future<?> flushFuture = null;
      if (readyToFlush) {
        flushFuture = submitFlush(drainBoundary, flushAction);
      }
      awaitFlush(
          drainBoundary,
          flushFuture,
          remainingNanos(startNanos, timeoutNanos),
          TimeUnit.NANOSECONDS);
      return awaitDrain(
          drainBoundary, remainingNanos(startNanos, timeoutNanos), TimeUnit.NANOSECONDS);
    } finally {
      abortDrain(drainBoundary);
    }
  }

  public Future<?> submitFlush(
      @Nonnull final DrainBoundary drainBoundary, @Nonnull final Runnable flushAction) {
    validateBoundary(drainBoundary);
    while (!flushInProgress.compareAndSet(false, true)) {
      // A flush from an earlier drain is still marked in progress. Because the executor clears
      // flushInProgress in its finally only after the flush body returns, the flag lags the actual
      // completion. Briefly await that in-flight flush: if it settles we retry the CAS and submit
      // our own flush (so its completion is not attributed to a spurious timeout); if it is still
      // running after the settle bound it is genuinely stuck, so record a timeout rather than queue
      // behind it (the caller redelivers idempotently).
      if (!awaitInFlightFlushSettled(inFlightFlush)) {
        recordFlushTimeout(drainBoundary);
        return null;
      }
    }
    final Future<?> flushFuture;
    try {
      flushFuture =
          flushExecutor.submit(
              () -> {
                try {
                  flushAction.run();
                } finally {
                  flushInProgress.set(false);
                }
              });
    } catch (RuntimeException e) {
      flushInProgress.set(false);
      recordFlushFailure(drainBoundary, e);
      return null;
    }
    inFlightFlush = flushFuture;
    return flushFuture;
  }

  /**
   * Waits up to {@link #FLUSH_SETTLE_BOUND_NANOS} for a previously-submitted flush to complete so
   * the flag it clears in its finally becomes visible. Returns true when there is nothing to wait
   * for or the flush completed (the caller retries the CAS and submits its own flush), false when
   * it is still running (the caller fast-fails so repeated drains do not queue behind a stuck
   * flush).
   */
  private static boolean awaitInFlightFlushSettled(@Nullable final Future<?> inFlight) {
    if (inFlight == null || inFlight.isDone()) {
      return true;
    }
    try {
      inFlight.get(FLUSH_SETTLE_BOUND_NANOS, TimeUnit.NANOSECONDS);
      return true;
    } catch (TimeoutException e) {
      return false;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (ExecutionException e) {
      // The in-flight flush itself failed; its own drain recorded that outcome. Nothing prevents us
      // from submitting our flush now.
      return true;
    }
  }

  public void awaitFlush(
      @Nonnull final DrainBoundary drainBoundary,
      final Future<?> flushFuture,
      final long timeout,
      @Nonnull final TimeUnit timeUnit) {
    if (flushFuture == null) {
      return;
    }
    try {
      flushFuture.get(Math.max(0, timeout), timeUnit);
    } catch (TimeoutException e) {
      recordFlushTimeout(drainBoundary);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      recordFlushFailure(drainBoundary, e);
    } catch (ExecutionException e) {
      recordFlushFailure(drainBoundary, e.getCause());
    }
  }

  public void shutdownFlushExecutor() {
    flushExecutor.shutdownNow();
  }

  @Nonnull
  public DrainBoundary beginDrain(final long timeout, @Nonnull final TimeUnit timeUnit) {
    lock.lock();
    try {
      // Bound the wait for a predecessor drain to release the slot against this drain's own flush
      // budget, so a stuck predecessor cannot hang every successor indefinitely. Track a deadline
      // (not the raw remaining) so a persistent interrupt cannot spin the loop, and preserve the
      // prior uninterruptible semantics by waiting out the budget and restoring the interrupt
      // after.
      final long deadlineNanos = System.nanoTime() + Math.max(0, timeUnit.toNanos(timeout));
      boolean interrupted = false;
      while (activeDrain != null) {
        final long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
          break;
        }
        try {
          completionChanged.awaitNanos(remainingNanos);
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
      final Thread drainThread = Thread.currentThread();
      if (activeDrain != null) {
        // The predecessor still holds the slot after the whole budget elapsed. Return a boundary
        // that reports a timeout without claiming the slot rather than blocking forever; the caller
        // surfaces a retryable timeout and redelivers.
        return DrainBoundary.notAcquired(this, drainThread);
      }
      final long boundary = nextSequence;
      OutcomeCounts boundaryOutcomes = undrainedOutcomesByThread.remove(drainThread);
      if (boundaryOutcomes == null) {
        boundaryOutcomes = new OutcomeCounts();
      }
      DrainBoundary drainBoundary =
          new DrainBoundary(this, boundary, drainThread, boundaryOutcomes);
      activeDrain = drainBoundary;
      return drainBoundary;
    } finally {
      lock.unlock();
    }
  }

  public boolean awaitSubmissions(
      @Nonnull final DrainBoundary drainBoundary,
      final long timeout,
      @Nonnull final TimeUnit timeUnit) {
    validateBoundary(drainBoundary);
    if (!drainBoundary.acquired) {
      return false;
    }
    lock.lock();
    try {
      long remainingNanos = Math.max(0, timeUnit.toNanos(timeout));
      while (hasSubmissionInProgressAtOrBefore(drainBoundary) && remainingNanos > 0) {
        try {
          remainingNanos = completionChanged.awaitNanos(remainingNanos);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          drainBoundary.flushException = e;
          return false;
        }
      }
      if (hasSubmissionInProgressAtOrBefore(drainBoundary)) {
        drainBoundary.timedOut = true;
        return false;
      }
      return drainBoundary.flushException == null;
    } finally {
      lock.unlock();
    }
  }

  public void abortDrain(@Nonnull final DrainBoundary drainBoundary) {
    validateBoundary(drainBoundary);
    lock.lock();
    try {
      if (drainBoundary.consumed) {
        return;
      }
      if (activeDrain != drainBoundary) {
        throw new IllegalStateException("Drain boundary is not active");
      }
      undrainedOutcomesByThread
          .computeIfAbsent(drainBoundary.thread, thread -> new OutcomeCounts())
          .mergeFrom(drainBoundary.outcomes);
      drainBoundary.consumed = true;
      activeDrain = null;
      completionChanged.signalAll();
    } finally {
      lock.unlock();
    }
  }

  public void recordFlushFailure(
      @Nonnull final DrainBoundary drainBoundary, @Nonnull final Throwable flushException) {
    validateBoundary(drainBoundary);
    lock.lock();
    try {
      drainBoundary.flushException = flushException;
      completionChanged.signalAll();
    } finally {
      lock.unlock();
    }
  }

  public void recordFlushTimeout(@Nonnull final DrainBoundary drainBoundary) {
    validateBoundary(drainBoundary);
    lock.lock();
    try {
      drainBoundary.timedOut = true;
      completionChanged.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @Nonnull
  public BulkProcessorFlushResult awaitDrain(
      @Nonnull final DrainBoundary drainBoundary,
      final long timeout,
      @Nonnull final TimeUnit timeUnit) {
    validateBoundary(drainBoundary);
    if (!drainBoundary.acquired) {
      // The slot was never acquired (beginDrain timed out); report a timeout without touching the
      // active drain held by the predecessor.
      return drainBoundary.outcomes.toResult(null, true);
    }
    lock.lock();
    try {
      if (drainBoundary.consumed) {
        throw new IllegalStateException("Drain boundary has already been consumed");
      }
      Throwable flushException = drainBoundary.flushException;
      long remainingNanos = Math.max(0, timeUnit.toNanos(timeout));
      while (flushException == null
          && !drainBoundary.timedOut
          && hasPendingAtOrBefore(drainBoundary)
          && remainingNanos > 0) {
        try {
          remainingNanos = completionChanged.awaitNanos(remainingNanos);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          flushException = e;
        }
      }
      if (flushException == null && hasPendingAtOrBefore(drainBoundary) && remainingNanos <= 0) {
        drainBoundary.timedOut = true;
      }
      drainBoundary.consumed = true;
      BulkProcessorFlushResult result = consume(drainBoundary, flushException);
      activeDrain = null;
      completionChanged.signalAll();
      return result;
    } finally {
      lock.unlock();
    }
  }

  public void recordOpenSearchResponse(
      @Nonnull final BulkRequest request, @Nonnull final BulkResponse response) {
    lock.lock();
    try {
      List<DocWriteRequest<?>> requests = request.requests();
      BulkItemResponse[] items = response.getItems();
      int completedItems = Math.min(requests.size(), items.length);
      for (int i = 0; i < completedItems; i++) {
        Submission submission = openSearchSubmissions.get(requests.get(i));
        if (submission != null) {
          complete(
              submission,
              classify(
                  items[i].getIndex(),
                  String.valueOf(items[i].getOpType()),
                  items[i].status().getStatus(),
                  items[i].isFailed()));
        }
      }
    } finally {
      lock.unlock();
    }
  }

  public void recordOpenSearchFailure(
      @Nonnull final BulkRequest request, @Nonnull final Throwable failure) {
    Outcome outcome = classifyRequestFailure(failure);
    lock.lock();
    try {
      for (DocWriteRequest<?> writeRequest : request.requests()) {
        Submission submission = openSearchSubmissions.get(writeRequest);
        if (submission != null) {
          complete(submission, outcome);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  public void recordEs8Response(
      @Nonnull final List<Object> contexts,
      @Nonnull final co.elastic.clients.elasticsearch.core.BulkResponse response) {
    lock.lock();
    try {
      int completedItems = Math.min(contexts.size(), response.items().size());
      for (int i = 0; i < completedItems; i++) {
        Submission submission = submissionFrom(contexts.get(i));
        if (submission != null) {
          co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem item =
              response.items().get(i);
          complete(
              submission,
              classify(
                  item.index(),
                  item.operationType() == null ? null : item.operationType().name(),
                  item.status(),
                  item.error() != null));
        }
      }
    } finally {
      lock.unlock();
    }
  }

  public void recordEs8Failure(
      @Nonnull final List<Object> contexts, @Nonnull final Throwable failure) {
    Outcome outcome = classifyRequestFailure(failure);
    lock.lock();
    try {
      for (Object context : contexts) {
        Submission submission = submissionFrom(context);
        if (submission != null) {
          complete(submission, outcome);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private Submission submissionFrom(Object context) {
    if (context instanceof Submission) {
      Submission submission = (Submission) context;
      if (submission.owner == this && !submission.consumed) {
        return submission;
      }
    }
    return null;
  }

  private static Outcome classifyRequestFailure(@Nonnull final Throwable failure) {
    if (isExpectedDocumentMissing(failure)) {
      return Outcome.EXPECTED_ABSENCE;
    }
    if (SearchIndexFailureClassifier.isRetryableTransportFailure(failure)) {
      return Outcome.TRANSPORT_FAILURE;
    }
    return Outcome.UNEXPECTED_ITEM_FAILURE;
  }

  private static boolean isExpectedDocumentMissing(@Nonnull final Throwable failure) {
    Throwable current = failure;
    while (current != null) {
      if (current instanceof DocumentMissingException) {
        return true;
      }
      if (current instanceof ElasticsearchException es
          && es.error() != null
          && BulkItemFailureClassifier.isDocumentMissing(es.error().type())) {
        return true;
      }
      if (BulkItemFailureClassifier.isDocumentMissing(current.getMessage())) {
        return true;
      }
      if (current == current.getCause()) {
        break;
      }
      current = current.getCause();
    }
    return false;
  }

  private static Outcome classify(
      String indexName, String operationType, int status, boolean failed) {
    if (V3BulkWriteAccounting.isSuccessfulV3IndexWrite(indexName, operationType, status)) {
      return Outcome.SUCCESSFUL_V3_WRITE;
    }
    if (failed) {
      if (V3BulkWriteAccounting.isExpectedV3StaleConflict(indexName, operationType, status)) {
        return Outcome.STALE_VERSION_REJECTED;
      }
      if (V3BulkWriteAccounting.isExpectedV3MissingDocument(indexName, operationType, status)) {
        return Outcome.SUCCESS;
      }
      // Item-level 429/5xx (e.g. es_rejected_execution_exception under write-queue backpressure) is
      // transient: a redelivery can land it. Classify it as a transport failure so the drain
      // escalates to RetryableIndexingException, matching how SearchIndexFailureClassifier treats
      // the same statuses for whole-request failures — the two halves must agree.
      if (SearchIndexFailureClassifier.isRetryableHttpStatus(status)) {
        return Outcome.TRANSPORT_FAILURE;
      }
      return Outcome.UNEXPECTED_ITEM_FAILURE;
    }
    return Outcome.SUCCESS;
  }

  private void complete(Submission submission, Outcome outcome) {
    if (!submission.consumed && submission.outcome == Outcome.PENDING) {
      submission.outcome = outcome;
      discard(submission);
      if (activeDrain != null
          && submission.batchThread == activeDrain.thread
          && submission.sequence <= activeDrain.sequence) {
        activeDrain.outcomes.record(outcome);
        if (outcome == Outcome.TRANSPORT_FAILURE || outcome == Outcome.UNEXPECTED_ITEM_FAILURE) {
          drainCountedRequests.add(submission.request);
        }
      } else {
        undrainedOutcomesByThread
            .computeIfAbsent(submission.batchThread, thread -> new OutcomeCounts())
            .record(outcome);
      }
      completionChanged.signalAll();
    }
  }

  /**
   * Whether {@code request}'s failure was already counted on the drain waiting for the thread that
   * submitted it. Removes the mark. A listener skips requeue when this is true: the drain fails
   * closed and the caller redelivers, so a callback-thread requeue would be a second write.
   */
  public boolean countedByDrain(@Nullable final DocWriteRequest<?> request) {
    if (request == null) {
      return false;
    }
    lock.lock();
    try {
      return drainCountedRequests.remove(request);
    } finally {
      lock.unlock();
    }
  }

  private boolean hasPendingAtOrBefore(DrainBoundary drainBoundary) {
    return submissions.headMap(drainBoundary.sequence, true).values().stream()
        .anyMatch(submission -> submission.batchThread == drainBoundary.thread);
  }

  private boolean hasSubmissionInProgressAtOrBefore(DrainBoundary drainBoundary) {
    return submissions.headMap(drainBoundary.sequence, true).values().stream()
        .anyMatch(
            submission ->
                submission.batchThread == drainBoundary.thread && submission.submissionInProgress);
  }

  private BulkProcessorFlushResult consume(DrainBoundary drainBoundary, Throwable flushException) {
    for (Submission submission :
        List.copyOf(submissions.headMap(drainBoundary.sequence, true).values())) {
      if (submission.batchThread != drainBoundary.thread) {
        continue;
      }
      drainBoundary.outcomes.incompleteOperations++;
      if (submission.outcome == Outcome.PENDING) {
        markDrainCounted(submission.request);
      }
      discard(submission);
    }

    return drainBoundary.outcomes.toResult(flushException, drainBoundary.timedOut);
  }

  /**
   * A write still pending when its drain gave up is counted incomplete, so the drain fails
   * retryable and the caller redelivers. Its late callback finds no submission and would otherwise
   * requeue a second copy. A write whose callback never arrives would keep its mark, so the set is
   * dropped when it grows past the bound; the cost is one redundant requeue.
   */
  private void markDrainCounted(DocWriteRequest<?> request) {
    if (drainCountedRequests.size() >= MAX_DRAIN_COUNTED_REQUESTS) {
      drainCountedRequests.clear();
    }
    drainCountedRequests.add(request);
  }

  private void discard(Submission submission) {
    submission.consumed = true;
    submissions.remove(submission.sequence);
    openSearchSubmissions.remove(submission.request, submission);
  }

  private void validateBoundary(DrainBoundary drainBoundary) {
    if (drainBoundary.owner != this) {
      throw new IllegalArgumentException("Drain boundary belongs to another tracker");
    }
  }

  private static long remainingNanos(long startNanos, long timeoutNanos) {
    return Math.max(0, timeoutNanos - (System.nanoTime() - startNanos));
  }

  int outstandingOperations() {
    lock.lock();
    try {
      return submissions.size();
    } finally {
      lock.unlock();
    }
  }

  private enum Outcome {
    PENDING,
    SUCCESS,
    SUCCESSFUL_V3_WRITE,
    STALE_VERSION_REJECTED,
    UNEXPECTED_ITEM_FAILURE,
    TRANSPORT_FAILURE,
    EXPECTED_ABSENCE
  }

  public static final class Submission {
    private final BulkProcessorCompletionTracker owner;
    private final long sequence;
    private final DocWriteRequest<?> request;
    private final Thread batchThread;
    private Outcome outcome = Outcome.PENDING;
    private boolean submissionInProgress = true;
    private boolean consumed;

    private Submission(
        BulkProcessorCompletionTracker owner, long sequence, DocWriteRequest<?> request) {
      this.owner = owner;
      this.sequence = sequence;
      this.request = request;
      this.batchThread = Thread.currentThread();
    }

    /** The write this submission is waiting on. The ES8 bulk context is this object. */
    @Nonnull
    public DocWriteRequest<?> getRequest() {
      return request;
    }
  }

  public static final class DrainBoundary {
    private final BulkProcessorCompletionTracker owner;
    private final long sequence;
    private final Thread thread;
    private final OutcomeCounts outcomes;
    // False for the sentinel returned when beginDrain could not claim the slot within its budget;
    // such a boundary owns no active drain, so the boundary-consuming methods short-circuit to a
    // timed-out result instead of touching the predecessor's state.
    private final boolean acquired;
    private Throwable flushException;
    private boolean timedOut;
    private boolean consumed;

    private DrainBoundary(
        BulkProcessorCompletionTracker owner,
        long sequence,
        Thread thread,
        OutcomeCounts outcomes) {
      this(owner, sequence, thread, outcomes, true);
    }

    private DrainBoundary(
        BulkProcessorCompletionTracker owner,
        long sequence,
        Thread thread,
        OutcomeCounts outcomes,
        boolean acquired) {
      this.owner = owner;
      this.sequence = sequence;
      this.thread = thread;
      this.outcomes = outcomes;
      this.acquired = acquired;
    }

    private static DrainBoundary notAcquired(BulkProcessorCompletionTracker owner, Thread thread) {
      DrainBoundary boundary = new DrainBoundary(owner, 0, thread, new OutcomeCounts(), false);
      boundary.timedOut = true;
      boundary.consumed = true;
      return boundary;
    }
  }

  private static final class OutcomeCounts {
    private long successfulV3Writes;
    private long staleVersionRejected;
    private long unexpectedItemFailures;
    private long transportFailedOperations;
    private long incompleteOperations;

    private void record(Outcome outcome) {
      switch (outcome) {
        case SUCCESSFUL_V3_WRITE:
          successfulV3Writes++;
          break;
        case STALE_VERSION_REJECTED:
          staleVersionRejected++;
          break;
        case UNEXPECTED_ITEM_FAILURE:
          unexpectedItemFailures++;
          break;
        case TRANSPORT_FAILURE:
          transportFailedOperations++;
          break;
        default:
          break;
      }
    }

    private void mergeFrom(OutcomeCounts other) {
      successfulV3Writes += other.successfulV3Writes;
      staleVersionRejected += other.staleVersionRejected;
      unexpectedItemFailures += other.unexpectedItemFailures;
      transportFailedOperations += other.transportFailedOperations;
      incompleteOperations += other.incompleteOperations;
    }

    private BulkProcessorFlushResult toResult(Throwable flushException, boolean timedOut) {
      return new BulkProcessorFlushResult(
          successfulV3Writes,
          staleVersionRejected,
          unexpectedItemFailures,
          transportFailedOperations,
          incompleteOperations,
          timedOut || (flushException == null && incompleteOperations > 0),
          flushException);
    }
  }
}
