package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SearchType;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.transport.TransportOptions;
import co.elastic.clients.transport.rest_client.RestClientOptions;
import com.datahub.context.OperationFingerprint;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import org.apache.http.client.config.RequestConfig;
import org.mockito.ArgumentCaptor;
import org.opensearch.client.RequestOptions;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.testng.annotations.Test;

public class Es8SearchClientShimSearchTypeTest {

  @Test
  public void searchSendsDfsSearchTypeOnlyWhenRequested() throws IOException {
    ElasticsearchClient mockClient = mock(ElasticsearchClient.class);
    ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
    SearchResponse<JsonNode> empty =
        SearchResponse.of(
            r ->
                r.took(1)
                    .timedOut(false)
                    .shards(s -> s.total(1).successful(1).failed(0))
                    .hits(h -> h.hits(List.of())));
    when(mockClient.search(captor.capture(), eq(JsonNode.class))).thenReturn(empty);
    Es8SearchClientShim shim = Es8SearchClientShim.forTest(mockClient);

    shim.search(
        OperationFingerprint.EMPTY,
        new org.opensearch.action.search.SearchRequest()
            .source(new SearchSourceBuilder())
            .searchType(org.opensearch.action.search.SearchType.DFS_QUERY_THEN_FETCH),
        RequestOptions.DEFAULT);
    shim.search(
        OperationFingerprint.EMPTY,
        new org.opensearch.action.search.SearchRequest().source(new SearchSourceBuilder()),
        RequestOptions.DEFAULT);

    List<SearchRequest> sent = captor.getAllValues();
    assertEquals(sent.get(0).searchType(), SearchType.DfsQueryThenFetch);
    assertNull(sent.get(1).searchType());
  }

  @Test
  public void searchAppliesPerRequestTimeouts() throws IOException {
    ElasticsearchClient mockClient = mock(ElasticsearchClient.class);
    ElasticsearchClient timedClient = mock(ElasticsearchClient.class);
    ArgumentCaptor<TransportOptions> transportOptions =
        ArgumentCaptor.forClass(TransportOptions.class);
    when(mockClient.withTransportOptions(transportOptions.capture())).thenReturn(timedClient);
    when(timedClient.search(any(SearchRequest.class), eq(JsonNode.class)))
        .thenReturn(
            SearchResponse.of(
                r ->
                    r.took(1)
                        .timedOut(false)
                        .shards(s -> s.total(1).successful(1).failed(0))
                        .hits(h -> h.hits(List.of()))));
    Es8SearchClientShim shim = Es8SearchClientShim.forTest(mockClient);

    shim.search(
        OperationFingerprint.EMPTY,
        new org.opensearch.action.search.SearchRequest().source(new SearchSourceBuilder()),
        RequestOptions.DEFAULT.toBuilder()
            .setRequestConfig(RequestConfig.custom().setSocketTimeout(1234).build())
            .build());

    verify(timedClient).search(any(SearchRequest.class), eq(JsonNode.class));
    RestClientOptions sentOptions = (RestClientOptions) transportOptions.getValue();
    assertEquals(
        sentOptions.restClientRequestOptions().getRequestConfig().getSocketTimeout(), 1234);
  }
}
