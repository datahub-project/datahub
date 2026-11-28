package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import com.linkedin.metadata.search.elasticsearch.update.BulkProcessorCompletionTracker;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.opensearch.action.DocWriteRequest;

/**
 * Bulk processor shim whose processor operations block or fail on demand, so shutdown and requeue
 * behavior can be driven deterministically.
 */
final class BlockingTestBulkProcessorShim extends AbstractBulkProcessorShim<Object> {

  /** Upper bound on any blocking wait, so a broken test cannot hang the build. */
  static final long CEILING_MILLIS = 10_000;

  final CountDownLatch release = new CountDownLatch(1);
  final CountDownLatch addStarted = new CountDownLatch(1);
  final CountDownLatch errorThrown = new CountDownLatch(1);
  final AtomicBoolean processorClosed = new AtomicBoolean();
  volatile boolean blockFlush;
  volatile boolean blockClose;
  volatile boolean blockAdd;
  volatile boolean failAdd;
  volatile boolean failAddWithError;

  BlockingTestBulkProcessorShim() {
    initBulkProcessors(1, tracker -> new Object());
  }

  @Override
  protected void addToProcessor(
      Object processor,
      DocWriteRequest<?> writeRequest,
      BulkProcessorCompletionTracker.Submission submission) {
    addStarted.countDown();
    if (blockAdd) {
      await(release);
    }
    if (failAddWithError) {
      errorThrown.countDown();
      throw new NoClassDefFoundError("simulated linkage failure");
    }
    if (failAdd) {
      throw new IllegalStateException("bulk process already closed");
    }
  }

  @Override
  protected void flushProcessor(Object processor) {
    if (blockFlush) {
      await(release);
    }
  }

  @Override
  protected void closeProcessor(Object processor) {
    if (blockClose) {
      await(release);
    }
    processorClosed.set(true);
  }

  void releaseBlocked() {
    release.countDown();
  }

  static void await(CountDownLatch latch) {
    try {
      latch.await(CEILING_MILLIS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
