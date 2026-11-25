package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.search.elasticsearch.update.BulkItemRequeueSupport;
import com.linkedin.metadata.search.elasticsearch.update.BulkProcessorCompletionTracker;
import com.linkedin.metadata.search.elasticsearch.update.BulkWriteResultTracker;
import com.linkedin.metadata.utils.elasticsearch.BulkProcessorFlushResult;
import java.time.Duration;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.DocWriteRequest;

/**
 * Abstract base class that provides common bulk processor functionality for search client shims.
 * This class handles the common patterns of managing multiple bulk processors with URN-based
 * consistent hashing.
 */
@Slf4j
public abstract class AbstractBulkProcessorShim<T> {

  protected int threadCount = 1;
  protected T[] bulkProcessors;
  protected BulkProcessorCompletionTracker[] completionTrackers;

  @Getter @Nonnull
  protected final BulkWriteResultTracker bulkWriteResultTracker = new BulkWriteResultTracker();

  protected boolean itemRequeueEnabled = true;
  protected int itemRequeueMaxAttempts = 3;

  @Nullable protected BulkItemRequeueSupport bulkItemRequeueSupport;

  /**
   * Initialize bulk processor infrastructure with common fields and build the processor array.
   * Subclasses should call this method with their processor supplier.
   */
  protected void initBulkProcessors(
      int threadCount, Function<BulkProcessorCompletionTracker, T> processorFactory) {
    this.threadCount = threadCount;
    this.bulkItemRequeueSupport =
        new BulkItemRequeueSupport(
            itemRequeueEnabled, itemRequeueMaxAttempts, this::requeueFailedRequest);

    @SuppressWarnings("unchecked")
    T[] processors = (T[]) new Object[threadCount];
    BulkProcessorCompletionTracker[] trackers = new BulkProcessorCompletionTracker[threadCount];
    for (int i = 0; i < threadCount; i++) {
      trackers[i] = new BulkProcessorCompletionTracker();
      processors[i] = processorFactory.apply(trackers[i]);
    }
    this.bulkProcessors = processors;
    this.completionTrackers = trackers;
  }

  public void configureBulkProcessorWriteOptions(
      boolean itemRequeueEnabled, int itemRequeueMaxAttempts) {
    this.itemRequeueEnabled = itemRequeueEnabled;
    this.itemRequeueMaxAttempts = itemRequeueMaxAttempts;
  }

  /**
   * Add a write request using URN-based consistent hashing for entity document consistency.
   * Subclasses must implement the actual processor-specific add logic.
   *
   * <p>The {@link OperationContext} is forwarded for wrapper-layer decoration (e.g. tenant routing
   * on the underlying write request). The base impl ignores it — bulk batching is intrinsically
   * cross-tenant, so per-request enrichment lives in the wrapper.
   */
  public void addBulk(
      @Nonnull OperationFingerprint opContext,
      @Nonnull String urn,
      @Nonnull DocWriteRequest<?> writeRequest) {
    bulkWriteResultTracker.recordEnqueued(1);
    int index = Math.floorMod(urn.hashCode(), threadCount);
    try {
      trackAndSubmit(index, writeRequest);
    } catch (RuntimeException | Error e) {
      bulkWriteResultTracker.recordUnrecoveredTransferFailure(1);
      throw e;
    }
  }

  /**
   * Flush all bulk processors. Subclasses must implement the actual processor-specific flush logic.
   */
  public void flushBulkProcessor() {
    if (bulkProcessors == null) {
      return;
    }
    for (T processor : bulkProcessors) {
      flushProcessor(processor);
    }
  }

  public void flushAndAwaitBulkTransfer(long timeoutMillis)
      throws InterruptedException, TimeoutException {
    flushBulkProcessor();
    bulkWriteResultTracker.awaitIdle(Duration.ofMillis(timeoutMillis));
  }

  public long drainBulkTransferFailures() {
    return bulkWriteResultTracker.drainUnrecoveredTransferFailures();
  }

  @Nonnull
  public BulkProcessorFlushResult flushBulkProcessorAndAwait(
      final long timeout, @Nonnull final TimeUnit timeUnit) {
    long timeoutNanos = Math.max(0, timeUnit.toNanos(timeout));
    long startNanos = System.nanoTime();
    BulkProcessorCompletionTracker.DrainBoundary[] boundaries =
        new BulkProcessorCompletionTracker.DrainBoundary[bulkProcessors.length];
    try {
      for (int i = 0; i < bulkProcessors.length; i++) {
        boundaries[i] =
            completionTrackers[i].beginDrain(
                remainingNanos(startNanos, timeoutNanos), TimeUnit.NANOSECONDS);
      }

      Future<?>[] flushFutures = new Future<?>[bulkProcessors.length];
      for (int i = 0; i < bulkProcessors.length; i++) {
        boolean readyToFlush =
            completionTrackers[i].awaitSubmissions(
                boundaries[i], remainingNanos(startNanos, timeoutNanos), TimeUnit.NANOSECONDS);
        if (readyToFlush) {
          final T processor = bulkProcessors[i];
          flushFutures[i] =
              completionTrackers[i].submitFlush(boundaries[i], () -> flushProcessor(processor));
        }
      }

      for (int i = 0; i < bulkProcessors.length; i++) {
        if (flushFutures[i] == null) {
          continue;
        }
        completionTrackers[i].awaitFlush(
            boundaries[i],
            flushFutures[i],
            remainingNanos(startNanos, timeoutNanos),
            TimeUnit.NANOSECONDS);
      }

      BulkProcessorFlushResult result = BulkProcessorFlushResult.empty();
      for (int i = 0; i < bulkProcessors.length; i++) {
        result =
            result.plus(
                completionTrackers[i].awaitDrain(
                    boundaries[i], remainingNanos(startNanos, timeoutNanos), TimeUnit.NANOSECONDS));
      }
      return result;
    } finally {
      for (int i = 0; i < boundaries.length; i++) {
        if (boundaries[i] != null) {
          completionTrackers[i].abortDrain(boundaries[i]);
        }
      }
    }
  }

  /**
   * Close all bulk processors. Subclasses must implement the actual processor-specific close logic.
   */
  public void closeBulkProcessor() {
    if (completionTrackers != null) {
      for (BulkProcessorCompletionTracker completionTracker : completionTrackers) {
        completionTracker.shutdownFlushExecutor();
      }
    }
    if (bulkProcessors == null) {
      return;
    }
    for (T processor : bulkProcessors) {
      closeProcessor(processor);
    }
  }

  /** Requeue without {@code recordEnqueued}: the item is already pending from the original add. */
  protected void requeueFailedRequest(@Nonnull DocWriteRequest<?> writeRequest) {
    if (bulkProcessors == null || bulkProcessors.length == 0 || completionTrackers == null) {
      log.warn("Cannot requeue bulk item; processors not initialized");
      return;
    }
    String routingKey =
        writeRequest.id() != null
            ? writeRequest.id()
            : String.valueOf(writeRequest.index()) + ":" + System.identityHashCode(writeRequest);
    int index = Math.floorMod(routingKey.hashCode(), threadCount);
    try {
      trackAndSubmit(index, writeRequest);
    } catch (RuntimeException | Error e) {
      bulkWriteResultTracker.recordUnrecoveredTransferFailure(1);
      throw e;
    }
  }

  private void trackAndSubmit(int index, @Nonnull DocWriteRequest<?> writeRequest) {
    completionTrackers[index].track(
        writeRequest,
        submission -> addToProcessor(bulkProcessors[index], writeRequest, submission));
  }

  private static long remainingNanos(long startNanos, long timeoutNanos) {
    return Math.max(0, timeoutNanos - (System.nanoTime() - startNanos));
  }

  /**
   * Add a write request to a specific processor. Subclasses must implement this method to handle
   * the specific processor type.
   */
  protected abstract void addToProcessor(
      T processor,
      DocWriteRequest<?> writeRequest,
      BulkProcessorCompletionTracker.Submission submission);

  /**
   * Flush a specific processor. Subclasses must implement this method to handle the specific
   * processor type.
   */
  protected abstract void flushProcessor(T processor);

  /**
   * Close a specific processor. Subclasses must implement this method to handle the specific
   * processor type.
   */
  protected abstract void closeProcessor(T processor);
}
