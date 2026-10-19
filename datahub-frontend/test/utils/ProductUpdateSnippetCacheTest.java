package utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

public class ProductUpdateSnippetCacheTest {

  @Test
  public void testConcurrentCallersDoNotWaitOnTheRefresh() throws Exception {
    CountDownLatch fetchStarted = new CountDownLatch(1);
    CountDownLatch releaseFetch = new CountDownLatch(1);
    AtomicInteger fetches = new AtomicInteger();
    ProductUpdateSnippetCache cache =
        new ProductUpdateSnippetCache(
            () -> {
              fetches.incrementAndGet();
              fetchStarted.countDown();
              try {
                assertTrue(releaseFetch.await(2, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              return "<script id=\"product-update\"></script>";
            },
            60_000);

    Thread refreshing = new Thread(cache::current);
    refreshing.start();
    assertTrue(fetchStarted.await(2, TimeUnit.SECONDS));

    assertEquals("", cache.current());
    releaseFetch.countDown();
    refreshing.join(2_000);

    assertEquals("<script id=\"product-update\"></script>", cache.current());
    assertEquals(1, fetches.get());
  }

  @Test
  public void testFailedRefreshKeepsThePreviousSnippet() {
    AtomicLong now = new AtomicLong(1_000);
    AtomicInteger fetches = new AtomicInteger();
    ProductUpdateSnippetCache cache =
        new ProductUpdateSnippetCache(
            () -> fetches.getAndIncrement() == 0 ? "<script></script>" : null, 10, now::get);

    assertEquals("<script></script>", cache.current());
    now.addAndGet(10);
    assertEquals("<script></script>", cache.current());
    assertEquals(2, fetches.get());
  }
}
