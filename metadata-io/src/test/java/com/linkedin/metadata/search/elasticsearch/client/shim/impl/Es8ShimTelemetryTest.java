package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.transport.TransportOptions;
import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.metadata.search.utils.ESUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestStats;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.util.List;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.core.CountRequest;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.testng.annotations.Test;

/** Request-attribution hooks in the ES8 shim: timing on search/count and the opaque-id header. */
public class Es8ShimTelemetryTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoValidate();

  private static SearchResponse<JsonNode> emptySearchResponse() {
    return SearchResponse.of(
        b ->
            b.took(3)
                .timedOut(false)
                .shards(s -> s.total(1).successful(1).failed(0))
                .hits(
                    h ->
                        h.hits(
                                List
                                    .<co.elastic.clients.elasticsearch.core.search.Hit<JsonNode>>
                                        of())
                            .total(t -> t.value(0).relation(TotalHitsRelation.Eq))));
  }

  @Test
  public void searchRecordsTimingAndSendsOpaqueIdInsideARequest() throws IOException {
    ElasticsearchClient client = mock(ElasticsearchClient.class);
    ArgumentCaptor<TransportOptions> optionsCaptor =
        ArgumentCaptor.forClass(TransportOptions.class);
    when(client.withTransportOptions(optionsCaptor.capture())).thenReturn(client);
    when(client.search(
            any(co.elastic.clients.elasticsearch.core.SearchRequest.class), eq(JsonNode.class)))
        .thenReturn(emptySearchResponse());
    Es8SearchClientShim shim = Es8SearchClientShim.forTest(client);

    RequestStats stats = new RequestStats(true);
    stats.attach(null, "urn:li:corpuser:jdoe", "searchAcrossEntities");
    Span span =
        Span.wrap(
            SpanContext.create(
                "0af7651916cd43dd8448eb211c80319c",
                "b7ad6b7169203331",
                TraceFlags.getSampled(),
                TraceState.getDefault()));
    SearchRequest request =
        new SearchRequest("datasetindex_v2")
            .source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(1));
    try (Scope ignored =
        Context.current().with(span).with(RequestStats.CONTEXT_KEY, stats).makeCurrent()) {
      shim.search(OP_CONTEXT, request, RequestOptions.DEFAULT);
    }

    assertEquals(stats.getEsCalls(), 1L);
    assertTrue(stats.getEsNanos() >= 0L);
    String header =
        optionsCaptor.getValue().headers().stream()
            .filter(e -> e.getKey().equalsIgnoreCase(ESUtils.OPAQUE_ID_HEADER))
            .map(java.util.Map.Entry::getValue)
            .findFirst()
            .orElse(null);
    assertEquals(
        header,
        "trace=0af7651916cd43dd8448eb211c80319c|actor=urn:li:corpuser:jdoe|req=searchAcrossEntities|n=1");
  }

  @Test
  public void countOutsideARequestUsesPlainClient() throws IOException {
    ElasticsearchClient client = mock(ElasticsearchClient.class);
    when(client.count(any(co.elastic.clients.elasticsearch.core.CountRequest.class)))
        .thenReturn(
            CountResponse.of(b -> b.count(7).shards(s -> s.total(1).successful(1).failed(0))));
    Es8SearchClientShim shim = Es8SearchClientShim.forTest(client);

    CountRequest request = new CountRequest("datasetindex_v2").query(QueryBuilders.matchAllQuery());
    assertEquals(shim.count(OP_CONTEXT, request, RequestOptions.DEFAULT).getCount(), 7L);
    // no request in scope and default options: the client is used as-is, no header layer
    verify(client, never()).withTransportOptions(any(TransportOptions.class));
  }

  @Test
  public void failedRoundTripsAreStillTimed() throws IOException {
    ElasticsearchClient client = mock(ElasticsearchClient.class);
    when(client.search(
            any(co.elastic.clients.elasticsearch.core.SearchRequest.class), eq(JsonNode.class)))
        .thenThrow(new IOException("search timed out"));
    when(client.count(any(co.elastic.clients.elasticsearch.core.CountRequest.class)))
        .thenThrow(new IOException("count timed out"));
    when(client.explain(
            any(co.elastic.clients.elasticsearch.core.ExplainRequest.class), eq(JsonNode.class)))
        .thenThrow(new IOException("explain timed out"));
    Es8SearchClientShim shim = Es8SearchClientShim.forTest(client);
    RequestStats stats = new RequestStats(false);
    SearchRequest search =
        new SearchRequest("idx")
            .source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
    CountRequest count = new CountRequest("idx").query(QueryBuilders.matchAllQuery());
    org.opensearch.action.explain.ExplainRequest explain =
        new org.opensearch.action.explain.ExplainRequest("idx", "doc1")
            .query(QueryBuilders.matchAllQuery());
    try (Scope ignored = Context.current().with(RequestStats.CONTEXT_KEY, stats).makeCurrent()) {
      for (Runnable call :
          List.<Runnable>of(
              () -> call(() -> shim.search(OP_CONTEXT, search, RequestOptions.DEFAULT)),
              () -> call(() -> shim.count(OP_CONTEXT, count, RequestOptions.DEFAULT)),
              () -> call(() -> shim.explain(OP_CONTEXT, explain, RequestOptions.DEFAULT)))) {
        call.run();
      }
    }
    assertEquals(stats.getEsCalls(), 3L, "search, count and explain each counted despite failing");
  }

  private interface Call {
    Object run() throws IOException;
  }

  private static void call(Call c) {
    try {
      c.run();
      throw new AssertionError("expected the failure to propagate");
    } catch (IOException expected) {
      // attributed, then propagated
    }
  }

  @Test
  public void explainSuccessIsTimed() throws IOException {
    ElasticsearchClient client = mock(ElasticsearchClient.class);
    co.elastic.clients.elasticsearch.core.ExplainResponse<JsonNode> response =
        co.elastic.clients.elasticsearch.core.ExplainResponse.of(
            b ->
                b.index("idx")
                    .id("doc1")
                    .matched(true)
                    .explanation(e -> e.value(1.0f).description("d").details(List.of())));
    when(client.explain(
            any(co.elastic.clients.elasticsearch.core.ExplainRequest.class), eq(JsonNode.class)))
        .thenReturn(response);
    Es8SearchClientShim shim = Es8SearchClientShim.forTest(client);
    RequestStats stats = new RequestStats(false);
    org.opensearch.action.explain.ExplainRequest explain =
        new org.opensearch.action.explain.ExplainRequest("idx", "doc1")
            .query(QueryBuilders.matchAllQuery());
    try (Scope ignored = Context.current().with(RequestStats.CONTEXT_KEY, stats).makeCurrent()) {
      assertTrue(shim.explain(OP_CONTEXT, explain, RequestOptions.DEFAULT).isMatch());
    }
    assertEquals(stats.getEsCalls(), 1L);
  }
}
