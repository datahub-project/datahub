package com.linkedin.metadata.search.elasticsearch.update;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;

/**
 * Tracks outstanding bulk write items and unrecovered transfer failures for flush-and-wait /
 * ack-after-transfer. An item stays pending from enqueue until a terminal outcome (success,
 * LWW-exhausted conflict, or unrecovered failure). Requeues do not change the pending count.
 */
public class BulkWriteResultTracker {
  private final AtomicInteger pendingItems = new AtomicInteger(0);
  private final AtomicLong unrecoveredTransferFailures = new AtomicLong(0);
  private final Object lock = new Object();

  public void recordEnqueued(int count) {
    if (count <= 0) {
      return;
    }
    pendingItems.addAndGet(count);
  }

  public void recordCompleted(int count) {
    if (count <= 0) {
      return;
    }
    pendingItems.addAndGet(-count);
    signal();
  }

  public void recordUnrecoveredTransferFailure(int count) {
    if (count <= 0) {
      return;
    }
    unrecoveredTransferFailures.addAndGet(count);
    recordCompleted(count);
  }

  public void recordLwwExhausted(int count) {
    recordCompleted(count);
  }

  public int getPendingItems() {
    return pendingItems.get();
  }

  public long getUnrecoveredTransferFailures() {
    return unrecoveredTransferFailures.get();
  }

  public long drainUnrecoveredTransferFailures() {
    return unrecoveredTransferFailures.getAndSet(0);
  }

  public boolean isIdle() {
    return pendingItems.get() <= 0;
  }

  public void awaitIdle(@Nonnull Duration timeout) throws InterruptedException, TimeoutException {
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    synchronized (lock) {
      while (!isIdle()) {
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0) {
          throw new TimeoutException(
              "Timed out waiting for bulk writes to complete. Pending items: "
                  + pendingItems.get());
        }
        TimeUnit.NANOSECONDS.timedWait(lock, remaining);
      }
    }
  }

  private void signal() {
    synchronized (lock) {
      lock.notifyAll();
    }
  }
}
