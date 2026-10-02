package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.search.elasticsearch.update.BulkItemRequeueSupport;
import com.linkedin.metadata.search.elasticsearch.update.BulkWriteResultTracker;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
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

  private static final AtomicInteger REQUEUE_THREAD_ID = new AtomicInteger();

  @Nullable private ExecutorService requeueExecutor;

  protected int threadCount = 1;
  protected T[] bulkProcessors;

  @Getter @Nonnull
  protected final BulkWriteResultTracker bulkWriteResultTracker = new BulkWriteResultTracker();

  protected boolean itemRequeueEnabled = true;
  protected int itemRequeueMaxAttempts = 3;

  @Nullable protected BulkItemRequeueSupport bulkItemRequeueSupport;

  /**
   * Initialize bulk processor infrastructure with common fields and build the processor array.
   * Subclasses should call this method with their processor supplier.
   */
  protected void initBulkProcessors(int threadCount, Supplier<T> processorSupplier) {
    initBulkProcessors(threadCount, processorSupplier, null);
  }

  /**
   * Like {@link #initBulkProcessors(int, Supplier)} but runs {@code afterRequeueReady} after {@link
   * BulkItemRequeueSupport} is constructed and before processors are built (so listeners can
   * capture it).
   */
  protected void initBulkProcessors(
      int threadCount, Supplier<T> processorSupplier, @Nullable Runnable afterRequeueReady) {
    this.threadCount = threadCount;
    // An unbounded queue keeps listener callbacks non-blocking. Never use CallerRunsPolicy here:
    // re-entering add() before the callback releases its in-flight permit can deadlock the
    // processor.
    this.requeueExecutor =
        Executors.newSingleThreadExecutor(
            task -> {
              Thread thread =
                  new Thread(task, "bulk-requeue-" + REQUEUE_THREAD_ID.incrementAndGet());
              thread.setDaemon(true);
              return thread;
            });
    this.bulkItemRequeueSupport =
        new BulkItemRequeueSupport(
            itemRequeueEnabled, itemRequeueMaxAttempts, this::requeueFailedRequest);
    if (afterRequeueReady != null) {
      afterRequeueReady.run();
    }

    @SuppressWarnings("unchecked")
    T[] processors = (T[]) new Object[threadCount];
    for (int i = 0; i < threadCount; i++) {
      processors[i] = processorSupplier.get();
    }
    this.bulkProcessors = processors;
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
    addToProcessor(bulkProcessors[index], writeRequest);
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

  /**
   * Close all bulk processors. Subclasses must implement the actual processor-specific close logic.
   */
  public void closeBulkProcessor() {
    boolean interrupted = false;
    try {
      if (requeueExecutor != null) {
        requeueExecutor.shutdown();
        // Drain accepted requeues before closing their processors. Late callbacks are rejected and
        // settled as failures below. Do not discard queued tasks, which still own pending items.
        while (!requeueExecutor.isTerminated()) {
          try {
            requeueExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
      }
      if (bulkProcessors != null) {
        for (T processor : bulkProcessors) {
          closeProcessor(processor);
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /** Requeue without {@code recordEnqueued} — item is already pending from the original add. */
  protected void requeueFailedRequest(@Nonnull DocWriteRequest<?> writeRequest) {
    if (requeueExecutor == null || bulkProcessors == null || bulkProcessors.length == 0) {
      recordRequeueFailure(
          writeRequest, new IllegalStateException("Bulk processors not initialized"));
      return;
    }
    try {
      requeueExecutor.execute(
          () -> {
            try {
              String routingKey =
                  writeRequest.id() != null
                      ? writeRequest.id()
                      : String.valueOf(writeRequest.index())
                          + ":"
                          + System.identityHashCode(writeRequest);
              int index = Math.floorMod(routingKey.hashCode(), threadCount);
              addToProcessor(bulkProcessors[index], writeRequest);
            } catch (RuntimeException e) {
              recordRequeueFailure(writeRequest, e);
            }
          });
    } catch (RejectedExecutionException e) {
      recordRequeueFailure(writeRequest, e);
    }
  }

  private void recordRequeueFailure(DocWriteRequest<?> writeRequest, RuntimeException failure) {
    log.warn(
        "Failed to requeue bulk item index [{}] id [{}]",
        writeRequest.index(),
        writeRequest.id(),
        failure);
    if (bulkItemRequeueSupport != null) {
      bulkItemRequeueSupport.clearAttempts(writeRequest);
    }
    // The listener leaves accepted retries pending; settle failures that it cannot observe.
    bulkWriteResultTracker.recordUnrecoveredTransferFailure(1);
  }

  /**
   * Add a write request to a specific processor. Subclasses must implement this method to handle
   * the specific processor type.
   */
  protected abstract void addToProcessor(T processor, DocWriteRequest<?> writeRequest);

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
