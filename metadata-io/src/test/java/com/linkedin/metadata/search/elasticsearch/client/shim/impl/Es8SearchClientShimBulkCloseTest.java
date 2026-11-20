package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.v8.Es8BulkListener;
import com.linkedin.metadata.search.elasticsearch.update.BulkProcessorCompletionTracker;
import com.linkedin.metadata.search.elasticsearch.update.BulkWriteResultTracker;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.testng.annotations.Test;

/** Shutdown of the ES8 bulk path: closing the processor waits for in-flight bulks. */
public class Es8SearchClientShimBulkCloseTest {

  private static final OperationFingerprint OP = OperationFingerprint.EMPTY;

  /** A real client whose transport hands back a future the test completes by hand. */
  private static ElasticsearchTransport heldTransport(
      AtomicReference<CompletableFuture<Object>> inFlight) {
    ElasticsearchTransport transport = mock(ElasticsearchTransport.class);
    when(transport.jsonpMapper()).thenReturn(new JacksonJsonpMapper());
    doAnswer(
            invocation -> {
              CompletableFuture<Object> future = new CompletableFuture<>();
              inFlight.set(future);
              return future;
            })
        .when(transport)
        .performRequestAsync(any(), any(), any());
    return transport;
  }

  private static BulkResponse oneItemSuccess() {
    return BulkResponse.of(
        b ->
            b.errors(false)
                .took(5)
                .items(
                    BulkResponseItem.of(
                        i ->
                            i.operationType(OperationType.Index)
                                .index("idx")
                                .id("1")
                                .status(201)
                                .result(Result.Created.jsonValue()))));
  }

  @Test
  public void closeWaitsForInFlightBulkBeforeClientCloses() throws Exception {
    AtomicReference<CompletableFuture<Object>> inFlight = new AtomicReference<>();
    ElasticsearchTransport transport = heldTransport(inFlight);
    Es8SearchClientShim shim = Es8SearchClientShim.forTest(new ElasticsearchClient(transport));
    ESBulkProcessor processor =
        ESBulkProcessor.builder(shim, mock(MetricUtils.class))
            .async(true)
            .writeRequestRefreshPolicy(WriteRequest.RefreshPolicy.NONE)
            .bulkRequestsLimit(10)
            .bulkFlushPeriod(3600)
            .numRetries(0)
            .build();

    shim.addBulk(OP, "urn:li:test:1", new IndexRequest("idx").id("1").source(Map.of("f", "v")));
    processor.flush();
    assertNotNull(inFlight.get(), "the bulk should be on the wire");

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> closing = executor.submit(() -> processor.close(Duration.ofSeconds(30)));
      assertThrows(TimeoutException.class, () -> closing.get(300, TimeUnit.MILLISECONDS));

      inFlight.get().complete(oneItemSuccess());
      closing.get(10, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }

    assertEquals(shim.getBulkWriteResultTracker().getPendingItems(), 0);
    assertEquals(shim.getBulkWriteResultTracker().getUnrecoveredTransferFailures(), 0L);
    verify(transport).performRequestAsync(any(), any(), any());
  }

  @Test
  public void transportFailureAfterCloseIsUnrecoveredAndDoesNotThrow() {
    Es8SearchClientShim shim =
        Es8SearchClientShim.forTest(
            new ElasticsearchClient(heldTransport(new AtomicReference<>())));
    shim.generateAsyncBulkProcessor(
        WriteRequest.RefreshPolicy.NONE, mock(MetricUtils.class), 10, 3600L, 1L, 0, 1);
    shim.closeBulkProcessor();

    BulkWriteResultTracker tracker = shim.getBulkWriteResultTracker();
    tracker.recordEnqueued(1);
    Es8BulkListener listener =
        new Es8BulkListener(
            mock(MetricUtils.class),
            (BulkProcessorCompletionTracker) null,
            tracker,
            shim.bulkItemRequeueSupport);
    IndexRequest write = new IndexRequest("idx").id("1").source(Map.of("f", "v"));
    BulkRequest request =
        BulkRequest.of(
            b ->
                b.operations(
                    op -> op.index(i -> i.index("idx").id("1").document(Map.of("f", "v")))));

    listener.afterBulk(1L, request, List.of(write), new IOException("connection closed"));

    assertEquals(tracker.getPendingItems(), 0);
    assertEquals(tracker.getUnrecoveredTransferFailures(), 1L);
  }
}
