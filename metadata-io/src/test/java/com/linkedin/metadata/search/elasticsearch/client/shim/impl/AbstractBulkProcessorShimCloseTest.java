package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static com.linkedin.metadata.search.elasticsearch.client.shim.impl.BlockingTestBulkProcessorShim.CEILING_MILLIS;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Shutdown must stay within its budget when a flush or a native close blocks. */
public class AbstractBulkProcessorShimCloseTest {

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
  public void testCloseReturnsWithinBudgetWhenNativeCloseBlocks() throws Exception {
    shim = new BlockingTestBulkProcessorShim();
    shim.blockClose = true;

    long started = System.nanoTime();
    shim.closeBulkProcessorWithin(200);

    assertTrue(elapsedMillis(started) < CEILING_MILLIS);
  }

  @Test
  public void testCloseTimesOutWhenFlushBlocksButStillClosesProcessors() throws Exception {
    shim = new BlockingTestBulkProcessorShim();
    shim.blockFlush = true;

    long started = System.nanoTime();
    assertThrows(TimeoutException.class, () -> shim.closeBulkProcessorWithin(200));

    assertTrue(elapsedMillis(started) < CEILING_MILLIS);
    awaitTrue(shim.processorClosed);
  }

  private static long elapsedMillis(long startedNanos) {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
  }

  private static void awaitTrue(java.util.concurrent.atomic.AtomicBoolean flag)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(CEILING_MILLIS);
    while (!flag.get() && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertTrue(flag.get(), "processors should still be closed after the flush timed out");
  }
}
