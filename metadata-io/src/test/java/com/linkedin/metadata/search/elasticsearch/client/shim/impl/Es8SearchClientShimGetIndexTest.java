package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ExpandWildcard;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.elasticsearch.indices.GetIndexRequest;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import com.datahub.context.OperationFingerprint;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.RequestOptions;
import org.testng.annotations.Test;

/**
 * A wildcard listing that matches nothing fails under the Elasticsearch client's defaults, so the
 * shared request's index options have to reach the Elasticsearch request.
 */
public class Es8SearchClientShimGetIndexTest {

  @Test
  public void getIndexForwardsIndicesOptions() throws IOException {
    ElasticsearchClient mockClient = mock(ElasticsearchClient.class);
    ElasticsearchIndicesClient indices = mock(ElasticsearchIndicesClient.class);
    when(mockClient.indices()).thenReturn(indices);
    ArgumentCaptor<GetIndexRequest> captor = ArgumentCaptor.forClass(GetIndexRequest.class);
    when(indices.get(captor.capture())).thenReturn(GetIndexResponse.of(r -> r.result(Map.of())));
    Es8SearchClientShim shim = Es8SearchClientShim.forTest(mockClient);

    shim.getIndex(
        OperationFingerprint.EMPTY,
        new org.opensearch.client.indices.GetIndexRequest("*index_v2*")
            .indicesOptions(IndicesOptions.lenientExpandOpen()),
        RequestOptions.DEFAULT);

    GetIndexRequest sent = captor.getValue();
    assertEquals(sent.index(), List.of("*index_v2*"));
    assertTrue(sent.allowNoIndices());
    assertTrue(sent.ignoreUnavailable());
    assertEquals(sent.expandWildcards(), List.of(ExpandWildcard.Open));
  }
}
