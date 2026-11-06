package com.linkedin.metadata.elasticsearch.update;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.search.elasticsearch.client.shim.impl.AbstractBulkProcessorShim;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.index.IndexRequest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BulkRequeueLifecycleTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(5);

  @Test
  public void testFailedAsyncAddSettlesPendingAndClearsAttempts() throws Exception {
    AtomicReference<Thread> worker = new AtomicReference<>();
    TestShim shim =
        new TestShim(
            request -> {
              worker.set(Thread.currentThread());
              throw new IllegalStateException("processor closed");
            },
            () -> {});
    try {
      assertFailuresSettleAndClearAttempts(shim);
      assertTrue(worker.get() != Thread.currentThread());
      assertTrue(worker.get().isDaemon());
    } finally {
      shim.closeBulkProcessor();
    }
  }

  @Test
  public void testRejectedRequeueSettlesPendingAndClearsAttempts() throws Exception {
    TestShim shim =
        new TestShim(
            request -> {
              throw new AssertionError("add after close");
            },
            () -> {});
    shim.closeBulkProcessor();
    assertFailuresSettleAndClearAttempts(shim);
  }

  private static void assertFailuresSettleAndClearAttempts(TestShim shim) throws Exception {
    DocWriteRequest<?> request = new IndexRequest("idx").id("a").source(Collections.emptyMap());
    // maxAttempts is one. Reusing the key proves the failed submission cleared its attempt state.
    for (int i = 0; i < 2; i++) {
      shim.getBulkWriteResultTracker().recordEnqueued(1);
      assertTrue(shim.retry(request));
      shim.getBulkWriteResultTracker().awaitIdle(TIMEOUT);
      assertEquals(shim.getBulkWriteResultTracker().getPendingItems(), 0);
      assertEquals(shim.drainBulkTransferFailures(), 1L);
    }
  }

  @DataProvider
  public Object[][] interrupts() {
    return new Object[][] {{false}, {true}};
  }

  @Test(dataProvider = "interrupts")
  public void testCloseDrainsAcceptedRequeuesBeforeClosingProcessor(boolean interrupt)
      throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    List<String> events = new CopyOnWriteArrayList<>();
    TestShim shim =
        new TestShim(
            request -> {
              entered.countDown();
              try {
                assertTrue(release.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
              }
              events.add(request.id());
            },
            () -> events.add("close"));
    FutureTask<Boolean> closing =
        new FutureTask<>(
            () -> {
              shim.closeBulkProcessor();
              return Thread.currentThread().isInterrupted();
            });
    Thread closer = new Thread(closing, "bulk-requeue-close-test");
    closer.setDaemon(true);
    try {
      shim.getBulkWriteResultTracker().recordEnqueued(2);
      assertTrue(shim.retry(new IndexRequest("idx").id("a")));
      assertTrue(entered.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
      assertTrue(shim.retry(new IndexRequest("idx").id("b")));
      closer.start();
      await()
          .atMost(TIMEOUT)
          .until(() -> closing.isDone() || closer.getState() == Thread.State.TIMED_WAITING);
      assertFalse(closing.isDone(), "close must wait for accepted requeues");
      assertTrue(events.isEmpty(), "the processor must remain open while a retry is blocked");
      if (interrupt) {
        closer.interrupt();
      }
      release.countDown();
      assertEquals(
          closing.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).booleanValue(), interrupt);
      assertEquals(events, List.of("a", "b", "close"));
      // Enqueueing the retries does not count them a second time or prematurely complete them.
      assertEquals(shim.getBulkWriteResultTracker().getPendingItems(), 2);
    } finally {
      release.countDown();
      closer.join(TIMEOUT.toMillis());
      shim.closeBulkProcessor();
    }
  }

  private static class TestShim extends AbstractBulkProcessorShim<Object> {
    private final Consumer<DocWriteRequest<?>> add;
    private final Runnable close;

    TestShim(Consumer<DocWriteRequest<?>> add, Runnable close) {
      this.add = add;
      this.close = close;
      configureBulkProcessorWriteOptions(true, 1);
      initBulkProcessors(1, Object::new);
    }

    boolean retry(DocWriteRequest<?> request) {
      return bulkItemRequeueSupport.tryRequeue(request);
    }

    @Override
    protected void addToProcessor(Object processor, DocWriteRequest<?> request) {
      add.accept(request);
    }

    @Override
    protected void flushProcessor(Object processor) {}

    @Override
    protected void closeProcessor(Object processor) {
      close.run();
    }
  }
}
