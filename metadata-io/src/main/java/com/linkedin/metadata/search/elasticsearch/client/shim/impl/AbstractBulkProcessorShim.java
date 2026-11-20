package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.search.elasticsearch.update.BulkItemRequeueSupport;
import com.linkedin.metadata.search.elasticsearch.update.BulkProcessorCompletionTracker;
import com.linkedin.metadata.search.elasticsearch.update.BulkWriteResultTracker;
import com.linkedin.metadata.utils.elasticsearch.BulkProcessorFlushResult;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
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

  private enum Lifecycle {
    OPEN,
    // A close owns the processors and is draining them; failed items may still be requeued.
    CLOSING,
    // Native processors are closed; any requeue would be dropped, so it is rejected instead.
    CLOSED
  }

  private final AtomicReference<Lifecycle> lifecycle = new AtomicReference<>(Lifecycle.OPEN);
  private volatile boolean receivedBulkWrites;

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
    lifecycle.set(Lifecycle.OPEN);
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
    receivedBulkWrites = true;
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

  public int pendingBulkWriteCount() {
    return bulkWriteResultTracker.getPendingItems();
  }

  @Nonnull
  public List<String> pendingBulkItems() {
    if (completionTrackers == null) {
      return List.of();
    }
    List<String> keys = new ArrayList<>();
    for (BulkProcessorCompletionTracker completionTracker : completionTrackers) {
      keys.addAll(completionTracker.pendingItemKeys());
    }
    return keys;
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
   * Claims the close of this shim's processors. Several {@code ESBulkProcessor}s can share one shim
   * (the Spring bean, the registry entry, aliased clusters); only the first claim drains them.
   *
   * @return false if another close already claimed or finished it
   */
  public boolean claimBulkProcessorClose() {
    return lifecycle.compareAndSet(Lifecycle.OPEN, Lifecycle.CLOSING);
  }

  public boolean hasReceivedBulkWrites() {
    return receivedBulkWrites;
  }

  /**
   * Close all bulk processors. Subclasses must implement the actual processor-specific close logic.
   */
  public void closeBulkProcessor() {
    lifecycle.set(Lifecycle.CLOSED);
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

  /**
   * Requeue without {@code recordEnqueued}: the item is already pending from the original add.
   * Throws when the item cannot be resubmitted; the listener then records it as unrecovered, so it
   * must not be recorded here as well.
   */
  protected void requeueFailedRequest(@Nonnull DocWriteRequest<?> writeRequest) {
    if (lifecycle.get() == Lifecycle.CLOSED) {
      throw new IllegalStateException("Cannot requeue bulk item; bulk processors are closed");
    }
    if (bulkProcessors == null || bulkProcessors.length == 0 || completionTrackers == null) {
      throw new IllegalStateException("Cannot requeue bulk item; processors not initialized");
    }
    String routingKey =
        writeRequest.id() != null
            ? writeRequest.id()
            : String.valueOf(writeRequest.index()) + ":" + System.identityHashCode(writeRequest);
    int index = Math.floorMod(routingKey.hashCode(), threadCount);
    trackAndSubmit(index, writeRequest);
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
