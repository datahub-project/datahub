package com.linkedin.gms.factory.common;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

/**
 * An {@link ExecutorService} that runs each task on its own virtual thread, but caps the number of
 * tasks running concurrently with a {@link Semaphore}. It gives the offloading hooks the same
 * bounded, never-drop, caller-runs contract as the platform {@code ThreadPoolExecutor} path, while
 * using cheap virtual threads instead of a fixed pool of platform threads.
 *
 * <p><b>Why a bound at all.</b> An unbounded virtual-thread-per-task executor would let a burst of
 * events fan out to an unbounded number of concurrent remote calls against a single downstream
 * (GMS, ES, the DB) — the classic virtual-thread footgun. The permit count is that downstream
 * concurrency limit.
 *
 * <p><b>Saturation = caller-runs, never drop.</b> When no permit is available the task runs inline
 * on the submitting (consumer) thread. This mirrors the platform pool's always-caller-runs handler:
 * committed work is never dropped, and under overload the consumer self-throttles to its own
 * service rate — the backpressure we want ahead of the Kafka offset commit. It also means a caller
 * that blocks on a barrier can always make progress even when every permit is held.
 *
 * <p><b>Not for pool threads.</b> Only the submitting thread may saturate-inline; tasks themselves
 * must never submit-and-join back into this executor, or an inline run could deadlock against its
 * own barrier. The hooks that use it submit leaf I/O only.
 */
public final class BoundedVirtualThreadExecutorService extends AbstractExecutorService {

  private final ExecutorService delegate;
  private final Semaphore permits;

  public BoundedVirtualThreadExecutorService(
      final int concurrency, @Nonnull final String threadPrefix) {
    // At least one permit; a non-positive bound would make every task run inline (pointless) or,
    // worse, block forever.
    this.permits = new Semaphore(Math.max(1, concurrency));
    final ThreadFactory factory = Thread.ofVirtual().name(threadPrefix, 0).factory();
    this.delegate = Executors.newThreadPerTaskExecutor(factory);
  }

  @Override
  public void execute(@Nonnull final Runnable command) {
    // tryAcquire (non-blocking): got a permit -> run on a virtual thread; otherwise run inline
    // (caller-runs backpressure). After shutdown, tryAcquire may still succeed but delegate.execute
    // throws RejectedExecutionException — fall back to inline so committed work is never dropped.
    if (permits.tryAcquire()) {
      try {
        delegate.execute(
            () -> {
              try {
                command.run();
              } finally {
                permits.release();
              }
            });
      } catch (RuntimeException e) {
        permits.release();
        command.run();
      }
    } else {
      command.run();
    }
  }

  @Override
  public void shutdown() {
    delegate.shutdown();
  }

  @Override
  @Nonnull
  public List<Runnable> shutdownNow() {
    return delegate.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return delegate.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return delegate.isTerminated();
  }

  @Override
  public boolean awaitTermination(final long timeout, @Nonnull final TimeUnit unit)
      throws InterruptedException {
    return delegate.awaitTermination(timeout, unit);
  }
}
