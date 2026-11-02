package com.linkedin.metadata.elasticsearch.update;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.metadata.search.elasticsearch.update.BulkWriteResultTracker;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

public class BulkWriteResultTrackerTest {

  @Test
  public void testPendingLifecycle() {
    BulkWriteResultTracker tracker = new BulkWriteResultTracker();
    assertTrue(tracker.isIdle());

    tracker.recordEnqueued(2);
    assertEquals(tracker.getPendingItems(), 2);
    assertFalse(tracker.isIdle());

    tracker.recordCompleted(1);
    assertEquals(tracker.getPendingItems(), 1);

    tracker.recordLwwExhausted(1);
    assertTrue(tracker.isIdle());
  }

  @Test
  public void testUnrecoveredFailuresDrain() {
    BulkWriteResultTracker tracker = new BulkWriteResultTracker();
    tracker.recordEnqueued(2);
    tracker.recordUnrecoveredTransferFailure(2);
    assertTrue(tracker.isIdle());
    assertEquals(tracker.getUnrecoveredTransferFailures(), 2);
    assertEquals(tracker.drainUnrecoveredTransferFailures(), 2);
    assertEquals(tracker.drainUnrecoveredTransferFailures(), 0);
  }

  @Test
  public void testRequeueKeepsPending() {
    BulkWriteResultTracker tracker = new BulkWriteResultTracker();
    tracker.recordEnqueued(1);
    // requeue does not call recordCompleted — still pending
    assertEquals(tracker.getPendingItems(), 1);
    tracker.recordCompleted(1);
    assertTrue(tracker.isIdle());
  }

  @Test
  public void testAwaitIdleTimeout() {
    BulkWriteResultTracker tracker = new BulkWriteResultTracker();
    tracker.recordEnqueued(1);
    expectThrows(TimeoutException.class, () -> tracker.awaitIdle(Duration.ofMillis(20)));
  }

  @Test
  public void testAwaitIdleReturnsWhenComplete() throws Exception {
    BulkWriteResultTracker tracker = new BulkWriteResultTracker();
    tracker.recordEnqueued(1);
    Thread completer =
        new Thread(
            () -> {
              try {
                Thread.sleep(30);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              tracker.recordCompleted(1);
            });
    completer.start();
    tracker.awaitIdle(Duration.ofSeconds(2));
    completer.join();
    assertTrue(tracker.isIdle());
  }
}
