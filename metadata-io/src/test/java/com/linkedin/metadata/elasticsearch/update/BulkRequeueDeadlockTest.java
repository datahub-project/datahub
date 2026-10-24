package com.linkedin.metadata.elasticsearch.update;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.AbstractBulkProcessorShim;
import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BulkRequeueDeadlockTest {
  private static final int TIMEOUT_SECONDS = 5;

  @DataProvider
  public Object[][] failures() {
    return new Object[][] {{false}, {true}};
  }

  @Test(dataProvider = "failures")
  public void testFailureCallbackReturnsWhileFlushWaitsForPermit(boolean itemFailure)
      throws Exception {
    BlockingQueue<ActionListener<BulkResponse>> inFlight = new LinkedBlockingQueue<>();
    AtomicInteger calls = new AtomicInteger();
    ProcessorShim shim =
        new ProcessorShim(
            1000,
            (request, listener) -> {
              if (calls.getAndIncrement() == 0) {
                inFlight.add(listener);
              } else {
                listener.onResponse(success(request));
              }
            });
    ExecutorService callers = newCallers();
    Future<?> flushing = null;
    try {
      shim.add("a");
      shim.flushBulkProcessor();
      ActionListener<BulkResponse> callback = inFlight.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      assertNotNull(callback);
      shim.add("b");
      AtomicReference<Thread> flushThread = new AtomicReference<>();
      flushing =
          callers.submit(
              () -> {
                flushThread.set(Thread.currentThread());
                shim.flushBulkProcessor();
              });
      // Observe the actual permit wait while flush owns the processor lock, without a timed race.
      await()
          .atMost(Duration.ofSeconds(TIMEOUT_SECONDS))
          .until(
              () ->
                  flushThread.get() != null
                      && Arrays.stream(flushThread.get().getStackTrace())
                          .anyMatch(
                              frame ->
                                  frame.getClassName().equals("java.util.concurrent.Semaphore")
                                      && frame.getMethodName().equals("acquire")));

      Future<?> failure =
          callers.submit(
              () -> {
                if (itemFailure) {
                  callback.onResponse(
                      new BulkResponse(
                          new BulkItemResponse[] {
                            new BulkItemResponse(
                                0,
                                DocWriteRequest.OpType.INDEX,
                                new BulkItemResponse.Failure(
                                    "idx",
                                    "a",
                                    new OpenSearchStatusException(
                                        "version_conflict_engine_exception", RestStatus.CONFLICT)))
                          },
                          1));
                } else {
                  callback.onFailure(new SocketTimeoutException());
                }
              });
      failure.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      flushing.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      assertTrue(shim.requeued.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
      shim.flushAndAwaitBulkTransfer(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
      assertEquals(shim.getBulkWriteResultTracker().getPendingItems(), 0);
      assertEquals(shim.drainBulkTransferFailures(), 0L);
    } finally {
      if (flushing != null) {
        flushing.cancel(true);
      }
      callers.shutdownNow();
      assertTrue(callers.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS));
      shim.closeBulkProcessor();
    }
  }

  @Test
  public void testInlineFailureDoesNotReenterPermitWaitAtBulkLimit() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    ProcessorShim shim =
        new ProcessorShim(
            1,
            (request, listener) -> {
              if (calls.getAndIncrement() == 0) {
                listener.onFailure(new SocketTimeoutException());
              } else {
                listener.onResponse(success(request));
              }
            });
    ExecutorService callers = newCallers();
    try {
      callers.submit(() -> shim.add("a")).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      shim.getBulkWriteResultTracker().awaitIdle(Duration.ofSeconds(TIMEOUT_SECONDS));
      assertEquals(shim.getBulkWriteResultTracker().getPendingItems(), 0);
      assertEquals(shim.drainBulkTransferFailures(), 0L);
      assertEquals(calls.get(), 2);
    } finally {
      callers.shutdownNow();
      assertTrue(callers.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS));
      shim.closeBulkProcessor();
    }
  }

  private static ExecutorService newCallers() {
    return Executors.newCachedThreadPool(
        task -> {
          Thread thread = new Thread(task, "bulk-requeue-test");
          thread.setDaemon(true);
          return thread;
        });
  }

  private static BulkResponse success(BulkRequest request) {
    BulkItemResponse[] items = new BulkItemResponse[request.numberOfActions()];
    for (int i = 0; i < items.length; i++) {
      items[i] = mock(BulkItemResponse.class);
      when(items[i].getItemId()).thenReturn(i);
      when(items[i].status()).thenReturn(RestStatus.OK);
    }
    return new BulkResponse(items, 1);
  }

  private static class ProcessorShim extends AbstractBulkProcessorShim<BulkProcessor> {
    private final AtomicInteger adds = new AtomicInteger();
    private final CountDownLatch requeued = new CountDownLatch(1);

    ProcessorShim(int bulkActions, BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer) {
      initBulkProcessors(
          1,
          () ->
              BulkProcessor.builder(
                      consumer,
                      BulkListener.create(
                          WriteRequest.RefreshPolicy.NONE,
                          null,
                          bulkWriteResultTracker,
                          bulkItemRequeueSupport))
                  .setConcurrentRequests(1)
                  .setBulkActions(bulkActions)
                  .setBackoffPolicy(BackoffPolicy.noBackoff())
                  .build());
    }

    void add(String id) {
      addBulk(
          OperationFingerprint.EMPTY,
          id,
          new IndexRequest("idx").id(id).source(Collections.emptyMap()));
    }

    @Override
    protected void addToProcessor(BulkProcessor processor, DocWriteRequest<?> request) {
      processor.add(request);
      if (adds.incrementAndGet() >= 3) {
        requeued.countDown();
      }
    }

    @Override
    protected void flushProcessor(BulkProcessor processor) {
      processor.flush();
    }

    @Override
    protected void closeProcessor(BulkProcessor processor) {
      processor.close();
    }
  }
}
