package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.DocWriteRequest;

/**
 * Abstract base class that provides common bulk processor functionality for search client shims.
 * This class handles the common patterns of managing multiple bulk processors with round-robin
 * distribution and URN-based consistent hashing.
 */
@Slf4j
public abstract class AbstractBulkProcessorShim<T> {

  protected int threadCount = 1;
  protected AtomicInteger roundRobinCounter;
  protected T[] bulkProcessors;

  /**
   * Initialize bulk processor infrastructure with common fields and build the processor array.
   * Subclasses should call this method with their processor supplier.
   */
  protected void initBulkProcessors(int threadCount, Supplier<T> processorSupplier) {
    this.threadCount = threadCount;
    this.roundRobinCounter = new AtomicInteger(0);

    @SuppressWarnings("unchecked")
    T[] processors = (T[]) new Object[threadCount];
    for (int i = 0; i < threadCount; i++) {
      processors[i] = processorSupplier.get();
    }
    this.bulkProcessors = processors;
  }

  /**
   * Add a write request using round-robin distribution across processors. Subclasses must implement
   * the actual processor-specific add logic.
   */
  public void addBulk(DocWriteRequest<?> writeRequest) {
    int index = roundRobinCounter.getAndIncrement() % threadCount;
    addToProcessor(bulkProcessors[index], writeRequest);
  }

  /**
   * Add a write request using URN-based consistent hashing for entity document consistency.
   * Subclasses must implement the actual processor-specific add logic.
   */
  public void addBulk(String urn, DocWriteRequest<?> writeRequest) {
    int index = Math.abs(urn.hashCode()) % threadCount;
    addToProcessor(bulkProcessors[index], writeRequest);
  }

  /**
   * Flush all bulk processors. Subclasses must implement the actual processor-specific flush logic.
   */
  public void flushBulkProcessor() {
    for (T processor : bulkProcessors) {
      flushProcessor(processor);
    }
  }

  /**
   * Close all bulk processors. Subclasses must implement the actual processor-specific close logic.
   */
  public void closeBulkProcessor() {
    for (T processor : bulkProcessors) {
      closeProcessor(processor);
    }
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
