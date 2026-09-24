package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import com.fasterxml.jackson.databind.JsonNode;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.List;
import org.mockito.ArgumentCaptor;
import org.opensearch.client.RequestOptions;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.testng.annotations.Test;

public class Es8SearchSizeTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  @Test
  public void explicitZeroSizeIsSent() throws IOException {
    assertEquals(sentSize(new SearchSourceBuilder().size(0)), Integer.valueOf(0));
  }

  @Test
  public void unsetSizeIsOmitted() throws IOException {
    assertNull(sentSize(new SearchSourceBuilder()));
  }

  private static Integer sentSize(SearchSourceBuilder source) throws IOException {
    ElasticsearchClient mockClient = mock(ElasticsearchClient.class);
    // search() round-trips the response through JSON, so it needs a real (empty) one, not a mock
    SearchResponse<JsonNode> emptyResponse =
        SearchResponse.of(
            b ->
                b.took(1)
                    .timedOut(false)
                    .shards(s -> s.total(1).successful(1).failed(0))
                    .hits(h -> h.hits(List.of())));
    ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
    when(mockClient.search(captor.capture(), eq(JsonNode.class))).thenReturn(emptyResponse);

    Es8SearchClientShim.forTest(mockClient)
        .search(
            OP_CONTEXT,
            new org.opensearch.action.search.SearchRequest("datasetindex_v2").source(source),
            RequestOptions.DEFAULT);

    return captor.getValue().size();
  }
}
