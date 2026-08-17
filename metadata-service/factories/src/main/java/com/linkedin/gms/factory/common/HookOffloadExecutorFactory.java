package com.linkedin.gms.factory.common;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The shared way MCL hooks move their independent per-event I/O off the Kafka consumer thread. It
 * owns two things:
 *
 * <ol>
 *   <li>{@link #build} — the reusable factory method every hook executor is built from, so each
 *       hook gets its <b>own</b> bounded pool with its <b>own</b> config namespace ({@code
 *       <hook>.executor.*}) and can independently choose a platform thread pool or virtual threads
 *       via {@code useVirtualThreads}. A hook declares its own bean by calling this method (see
 *       {@code siblingsHookExecutor} below for the template; a hook defined outside this module
 *       declares its bean in its own factory that calls this method).
 *   <li>The {@code SiblingAssociationHook} executor bean.
 * </ol>
 *
 * <p>The consumer always joins the offloaded work before the hook returns, so this changes latency,
 * not the at-most-once / ordering semantics. {@code MetadataTestHook} predates this factory and
 * keeps its own bounded pool + coalescing buffer (it needs batching/settle semantics this factory
 * does not model).
 *
 * <p><b>Two executor shapes, one contract.</b> Both are bounded and never drop:
 *
 * <ul>
 *   <li><b>Platform</b> — a {@link ThreadPoolExecutor} with an {@link ArrayBlockingQueue} and an
 *       always-caller-runs saturation handler. We deliberately avoid {@link
 *       ThreadPoolExecutor.CallerRunsPolicy}: it drops the task after shutdown, which with a
 *       barrier {@code join()} hangs the consumer on graceful shutdown. Running inline
 *       unconditionally never drops and never hangs.
 *   <li><b>Virtual</b> — a {@link BoundedVirtualThreadExecutorService}: a virtual thread per task,
 *       capped by a semaphore, saturating inline the same way. Better for spiky, rare, purely
 *       I/O-bound fan-out (e.g. a delete cascade) where a standing platform pool would mostly idle.
 * </ul>
 */
@Slf4j
@Configuration
public class HookOffloadExecutorFactory {

  /** Pool for {@code SiblingAssociationHook} (platform by default — steady dbt-driven writes). */
  public static final String BEAN_NAME_SIBLINGS = "siblingsHookExecutor";

  private static final RejectedExecutionHandler ALWAYS_CALLER_RUNS =
      (runnable, executor) -> runnable.run();

  // SiblingAssociationHook — platform pool for its independent ingestProposal writes. Keys sit
  // under
  // the existing `siblings.*` namespace (no `.hook.` segment, matching siblings.enabled).
  @Value("${siblings.executor.concurrency:4}")
  private int siblingsConcurrency;

  @Value("${siblings.executor.queueSize:500}")
  private int siblingsQueueSize;

  @Value("${siblings.executor.keepAliveSeconds:60}")
  private long siblingsKeepAliveSeconds;

  @Value("${siblings.executor.useVirtualThreads:false}")
  private boolean siblingsUseVirtualThreads;

  @Bean(name = BEAN_NAME_SIBLINGS, destroyMethod = "shutdown")
  @Nonnull
  protected ExecutorService siblingsHookExecutor() {
    return build(
        "datahub-hook-siblings-",
        siblingsConcurrency,
        siblingsQueueSize,
        siblingsKeepAliveSeconds,
        siblingsUseVirtualThreads);
  }

  /**
   * Builds a bounded, never-drop offload executor. With {@code useVirtualThreads=false} it is a
   * platform {@link ThreadPoolExecutor} (queue + always-caller-runs); with {@code true} it is a
   * {@link BoundedVirtualThreadExecutorService} (semaphore-capped virtual threads), in which case
   * {@code queueSize}/{@code keepAliveSeconds} do not apply. Register the result as a bean with
   * {@code destroyMethod = "shutdown"} so Spring shuts it down.
   */
  @Nonnull
  public static ExecutorService build(
      @Nonnull final String threadPrefix,
      final int concurrency,
      final int queueSize,
      final long keepAliveSeconds,
      final boolean useVirtualThreads) {
    // core<0, max<=0, or core>max would throw and fail context startup; clamp to a valid range so a
    // misconfig degrades gracefully. queueSize is clamped to >=1 (ArrayBlockingQueue requires it).
    final int effectiveConcurrency = Math.max(1, concurrency);
    if (effectiveConcurrency != concurrency) {
      log.warn("{} concurrency={} is invalid (min 1); using 1", threadPrefix, concurrency);
    }

    if (useVirtualThreads) {
      // queueSize/keepAlive do not apply: there is no fixed pool and no bounded queue — the
      // semaphore is the only bound, and idle virtual threads cost nothing. Log it so an operator
      // who tuned those keys is not surprised they have no effect.
      log.info(
          "{} using virtual threads (concurrency={}); queueSize and keepAliveSeconds are ignored",
          threadPrefix,
          effectiveConcurrency);
      return new BoundedVirtualThreadExecutorService(effectiveConcurrency, threadPrefix);
    }

    final int effectiveQueueSize = Math.max(1, queueSize);
    if (effectiveQueueSize != queueSize) {
      log.warn("{} queueSize={} is invalid (min 1); using 1", threadPrefix, queueSize);
    }
    final ThreadPoolExecutor executor =
        new ThreadPoolExecutor(
            effectiveConcurrency,
            effectiveConcurrency,
            keepAliveSeconds,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(effectiveQueueSize),
            daemonThreadFactory(threadPrefix),
            ALWAYS_CALLER_RUNS);
    // Let the pool shrink to zero when idle so it costs nothing between the (infrequent) events
    // these hooks fire on.
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  private static ThreadFactory daemonThreadFactory(@Nonnull final String threadPrefix) {
    final AtomicLong counter = new AtomicLong();
    return runnable -> {
      final Thread thread = new Thread(runnable, threadPrefix + counter.getAndIncrement());
      thread.setDaemon(true);
      return thread;
    };
  }
}
