package auth.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PlayHttpServerMetricsTest {

  @Test
  void forkJoinPool_tracksBusyQueuedAndConfig() throws Exception {
    ForkJoinPool pool = new ForkJoinPool(1);
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    try {
      PlayHttpServerMetrics.register(registry, pool, poolConfig(), new AtomicInteger(3));
      assertEquals(8, gauge(registry, PlayHttpServerMetrics.THREADS_CONFIG_MIN));
      assertEquals(64, gauge(registry, PlayHttpServerMetrics.THREADS_CONFIG_MAX));
      assertEquals(1024, gauge(registry, PlayHttpServerMetrics.CONNECTIONS_MAX));
      assertEquals(100, gauge(registry, PlayHttpServerMetrics.CONNECTIONS_BACKLOG));
      assertEquals(1, gauge(registry, PlayHttpServerMetrics.THREADS_PARALLELISM));
      assertEquals(3, gauge(registry, PlayHttpServerMetrics.REQUESTS_INFLIGHT));

      pool.submit(
          () -> {
            started.countDown();
            release.await();
            return null;
          });
      assertTrue(started.await(5, TimeUnit.SECONDS));
      pool.submit(() -> null);

      awaitGaugeAtLeast(registry, PlayHttpServerMetrics.THREADS_BUSY, 1);
      awaitGaugeAtLeast(registry, PlayHttpServerMetrics.THREADS_JOBS, 1);
      assertEquals(
          Math.max(
              0,
              gauge(registry, PlayHttpServerMetrics.THREADS_CURRENT)
                  - gauge(registry, PlayHttpServerMetrics.THREADS_BUSY)),
          gauge(registry, PlayHttpServerMetrics.THREADS_IDLE));
    } finally {
      release.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void threadPoolExecutor_tracksBusyQueueAndMax() throws Exception {
    ThreadPoolExecutor pool =
        new ThreadPoolExecutor(1, 2, 1, TimeUnit.SECONDS, new ArrayBlockingQueue<>(10));
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    try {
      PlayHttpServerMetrics.register(registry, pool, poolConfig(), new AtomicInteger(0));
      assertEquals(2, gauge(registry, PlayHttpServerMetrics.THREADS_PARALLELISM));

      pool.submit(
          () -> {
            started.countDown();
            release.await();
            return null;
          });
      assertTrue(started.await(5, TimeUnit.SECONDS));

      awaitGaugeAtLeast(registry, PlayHttpServerMetrics.THREADS_BUSY, 1);
      assertEquals(pool.getActiveCount(), gauge(registry, PlayHttpServerMetrics.THREADS_BUSY));
      assertEquals(pool.getPoolSize(), gauge(registry, PlayHttpServerMetrics.THREADS_CURRENT));
      assertEquals(pool.getQueue().size(), gauge(registry, PlayHttpServerMetrics.THREADS_JOBS));
      assertEquals(
          Math.max(0, pool.getPoolSize() - pool.getActiveCount()),
          gauge(registry, PlayHttpServerMetrics.THREADS_IDLE));
    } finally {
      release.countDown();
      pool.shutdownNow();
      assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void unsupportedExecutor_skipsThreadGaugesAndKeepsLimits() {
    ExecutorService executor = mock(ExecutorService.class);
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    PlayHttpServerMetrics.register(registry, executor, poolConfig(), new AtomicInteger(1));

    assertNull(registry.find(PlayHttpServerMetrics.THREADS_BUSY).gauge());
    assertNull(registry.find(PlayHttpServerMetrics.THREADS_JOBS).gauge());
    assertEquals(1024, gauge(registry, PlayHttpServerMetrics.CONNECTIONS_MAX));
    assertEquals(1, gauge(registry, PlayHttpServerMetrics.REQUESTS_INFLIGHT));
  }

  @Test
  void missingConfig_skipsUnsetLimitGauges() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    PlayHttpServerMetrics.register(registry, null, ConfigFactory.empty(), new AtomicInteger(0));

    assertNull(registry.find(PlayHttpServerMetrics.THREADS_BUSY).gauge());
    assertNull(registry.find(PlayHttpServerMetrics.THREADS_CONFIG_MAX).gauge());
    assertNull(registry.find(PlayHttpServerMetrics.CONNECTIONS_MAX).gauge());
    assertEquals(0, gauge(registry, PlayHttpServerMetrics.REQUESTS_INFLIGHT));
  }

  private static Config poolConfig() {
    return ConfigFactory.parseMap(
        Map.of(
            PlayHttpServerMetrics.PARALLELISM_MIN_PATH,
            8,
            PlayHttpServerMetrics.PARALLELISM_MAX_PATH,
            64,
            PlayHttpServerMetrics.MAX_CONNECTIONS_PATH,
            1024,
            PlayHttpServerMetrics.BACKLOG_PATH,
            100));
  }

  private static double gauge(SimpleMeterRegistry registry, String name) {
    return registry.get(name).gauge().value();
  }

  private static void awaitGaugeAtLeast(SimpleMeterRegistry registry, String name, double min)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (gauge(registry, name) < min) {
      if (System.nanoTime() > deadline) {
        fail(name + " stayed below " + min);
      }
      Thread.sleep(10);
    }
  }
}
