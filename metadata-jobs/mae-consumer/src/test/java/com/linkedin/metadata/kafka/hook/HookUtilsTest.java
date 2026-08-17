package com.linkedin.metadata.kafka.hook;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.fail;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.MDC;
import org.testng.annotations.Test;

/** Covers the offload primitives the MCL hooks rely on, on real (multi-threaded) executors. */
public class HookUtilsTest {

  @Test
  public void unwrapJoinReturnsValue() {
    assertEquals(HookUtils.unwrapJoin(CompletableFuture.completedFuture("x")), "x");
  }

  @Test
  public void unwrapJoinRethrowsOriginalRuntimeExceptionNotCompletionException() {
    final IllegalStateException boom = new IllegalStateException("boom");
    try {
      HookUtils.unwrapJoin(CompletableFuture.failedFuture(boom));
      fail("expected the original exception to propagate");
    } catch (RuntimeException e) {
      // The whole point of unwrapJoin: the caller sees `boom`, not a CompletionException wrapper,
      // so the listener's at-most-once handling behaves exactly as for inline execution.
      assertSame(e, boom);
    }
  }

  @Test
  public void awaitAllPropagatesFirstFailureAfterWaitingForAll() {
    final CompletableFuture<Void> ok = CompletableFuture.completedFuture(null);
    final CompletableFuture<Void> bad =
        CompletableFuture.failedFuture(new IllegalStateException("bad"));
    try {
      HookUtils.awaitAll(ok, bad);
      fail("expected the failure to propagate");
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "bad");
    }
  }

  @Test
  public void awaitAllReturnsWhenAllSucceed() {
    HookUtils.awaitAll(
        CompletableFuture.completedFuture(null), CompletableFuture.completedFuture(null));
  }

  @Test
  public void runAsyncPropagatesCallerMdcToWorkerThread() throws Exception {
    final ExecutorService pool = Executors.newSingleThreadExecutor();
    MDC.put("entityUrn", "urn:li:dataset:test");
    try {
      final AtomicReference<String> seenOnWorker = new AtomicReference<>();
      HookUtils.runAsync(() -> seenOnWorker.set(MDC.get("entityUrn")), pool).join();
      assertEquals(seenOnWorker.get(), "urn:li:dataset:test");
    } finally {
      MDC.clear();
      pool.shutdownNow();
    }
  }

  @Test
  public void runAsyncRestoresWorkerContextAfterTask() throws Exception {
    // A pooled worker thread that already had its own MDC must get it back after the task, so a
    // reused thread does not leak one event's context into the next.
    final ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      // Seed the worker thread with its own context.
      pool.submit(() -> MDC.put("worker", "original")).get();
      MDC.put("caller", "value");
      try {
        HookUtils.runAsync(() -> {}, pool).join();
      } finally {
        MDC.clear();
      }
      final AtomicReference<String> workerAfter = new AtomicReference<>();
      final AtomicReference<String> callerLeaked = new AtomicReference<>("present");
      pool.submit(
              () -> {
                workerAfter.set(MDC.get("worker"));
                callerLeaked.set(MDC.get("caller"));
              })
          .get();
      assertEquals(workerAfter.get(), "original");
      assertNull(callerLeaked.get());
    } finally {
      pool.shutdownNow();
    }
  }
}
