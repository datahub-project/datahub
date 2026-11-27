package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.search.elasticsearch.update.BulkProcessorCompletionTracker;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.opensearch.action.DocWriteRequest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Shutdown must stay within its budget when a flush or a native close blocks. */
public class AbstractBulkProcessorShimCloseTest {

  /** Generous ceiling so the test stays stable on slow CI; the budget is 200 ms. */
  private static final long CEILING_MILLIS = 10_000;

  // TestNG reuses one instance for every method, so per-test state is reset in @BeforeMethod.
  private CountDownLatch release;
  private AtomicBoolean processorClosed;
  private boolean blockFlush;
  private boolean blockClose;

  private final class TestShim extends AbstractBulkProcessorShim<Object> {
    TestShim() {
      initBulkProcessors(1, tracker -> new Object());
    }

    @Override
    protected void addToProcessor(
        Object processor,
        DocWriteRequest<?> writeRequest,
        BulkProcessorCompletionTracker.Submission submission) {}

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
  }

  @BeforeMethod
  public void resetState() {
    release = new CountDownLatch(1);
    processorClosed = new AtomicBoolean();
    blockFlush = false;
    blockClose = false;
  }

  @AfterMethod
  public void releaseBlockedThreads() {
    release.countDown();
  }

  @Test
  public void testCloseReturnsWithinBudgetWhenNativeCloseBlocks() throws Exception {
    blockClose = true;
    TestShim shim = new TestShim();

    long started = System.nanoTime();
    shim.closeBulkProcessorWithin(200);

    assertTrue(elapsedMillis(started) < CEILING_MILLIS);
  }

  @Test
  public void testCloseTimesOutWhenFlushBlocksButStillClosesProcessors() throws Exception {
    blockFlush = true;
    TestShim shim = new TestShim();

    long started = System.nanoTime();
    assertThrows(TimeoutException.class, () -> shim.closeBulkProcessorWithin(200));

    assertTrue(elapsedMillis(started) < CEILING_MILLIS);
    awaitTrue(processorClosed);
  }

  private static long elapsedMillis(long startedNanos) {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await(CEILING_MILLIS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void awaitTrue(AtomicBoolean flag) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(CEILING_MILLIS);
    while (!flag.get() && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertTrue(flag.get(), "processors should still be closed after the flush timed out");
  }
}
