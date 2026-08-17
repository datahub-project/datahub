package com.linkedin.gms.factory.common;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

public class BoundedVirtualThreadExecutorServiceTest {

  @Test
  public void runsEverySubmittedTask() {
    final ExecutorService ex = new BoundedVirtualThreadExecutorService(4, "test-vt-run-");
    try {
      final AtomicInteger count = new AtomicInteger();
      final CompletableFuture<?>[] futures = new CompletableFuture<?>[50];
      for (int i = 0; i < futures.length; i++) {
        futures[i] = CompletableFuture.runAsync(count::incrementAndGet, ex);
      }
      CompletableFuture.allOf(futures).join();
      assertEquals(count.get(), 50);
    } finally {
      ex.shutdown();
    }
  }

  @Test(timeOut = 5000)
  public void saturationRunsInlineWithoutDropOrDeadlock() throws Exception {
    // concurrency=1: park a task that holds the only permit, then submit a second task. With no
    // permit free it must run INLINE on the caller and complete even while the first is still
    // parked — proving the saturation path never drops and never blocks the caller — and it must
    // fire the onSaturation callback exactly once (the caller-runs metric hook).
    final AtomicInteger saturationCount = new AtomicInteger();
    final ExecutorService ex =
        new BoundedVirtualThreadExecutorService(
            1, "test-vt-sat-", saturationCount::incrementAndGet);
    try {
      final CountDownLatch parked = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      ex.execute(
          () -> {
            parked.countDown();
            try {
              release.await();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          });
      assertTrue(parked.await(2, TimeUnit.SECONDS), "first task should have started");

      final String callerThread = Thread.currentThread().getName();
      final AtomicInteger ranInline = new AtomicInteger();
      ex.execute(
          () -> {
            if (Thread.currentThread().getName().equals(callerThread)) {
              ranInline.incrementAndGet();
            }
          });
      assertEquals(ranInline.get(), 1, "saturated submission must run inline on the caller");
      assertEquals(saturationCount.get(), 1, "the inline run must record one saturation event");
      release.countDown();
    } finally {
      ex.shutdown();
    }
  }

  @Test(timeOut = 5000)
  public void permitReleasedAfterTaskCompletes() {
    // After a task finishes, its permit must be returned; otherwise a leak would force every
    // subsequent task inline forever. concurrency=1: run one task, then verify the next runs on a
    // (non-caller) virtual thread.
    final ExecutorService ex = new BoundedVirtualThreadExecutorService(1, "test-vt-rel-");
    try {
      CompletableFuture.runAsync(() -> {}, ex).join();

      final String callerThread = Thread.currentThread().getName();
      final AtomicInteger ranOffCaller = new AtomicInteger();
      CompletableFuture.runAsync(
              () -> {
                if (!Thread.currentThread().getName().equals(callerThread)) {
                  ranOffCaller.incrementAndGet();
                }
              },
              ex)
          .join();
      assertEquals(ranOffCaller.get(), 1, "permit should be free, so the task runs on a VT");
    } finally {
      ex.shutdown();
    }
  }
}
