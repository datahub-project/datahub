package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.search.elasticsearch.update.BulkItemRequeueSupport;
import com.linkedin.metadata.search.elasticsearch.update.BulkWriteResultTracker;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
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
    if (bulkProcessors == null) {
      return;
    }
    for (T processor : bulkProcessors) {
      closeProcessor(processor);
    }
  }

  /** Requeue without {@code recordEnqueued} — item is already pending from the original add. */
  protected void requeueFailedRequest(@Nonnull DocWriteRequest<?> writeRequest) {
    if (bulkProcessors == null || bulkProcessors.length == 0) {
      log.warn("Cannot requeue bulk item; processors not initialized");
      return;
    }
    String routingKey =
        writeRequest.id() != null
            ? writeRequest.id()
            : String.valueOf(writeRequest.index()) + ":" + System.identityHashCode(writeRequest);
    int index = Math.floorMod(routingKey.hashCode(), threadCount);
    addToProcessor(bulkProcessors[index], writeRequest);
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
