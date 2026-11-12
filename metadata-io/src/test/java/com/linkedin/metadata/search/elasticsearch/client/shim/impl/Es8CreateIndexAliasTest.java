package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import com.datahub.context.OperationFingerprint;
import java.io.IOException;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.client.RequestOptions;
import org.testng.annotations.Test;

public class Es8CreateIndexAliasTest {

  @Test
  public void createIndexForwardsAliases() throws IOException {
    ElasticsearchClient mockClient = mock(ElasticsearchClient.class);
    ElasticsearchIndicesClient mockIndices = mock(ElasticsearchIndicesClient.class);
    when(mockClient.indices()).thenReturn(mockIndices);
    co.elastic.clients.elasticsearch.indices.CreateIndexResponse fakeResponse =
        co.elastic.clients.elasticsearch.indices.CreateIndexResponse.of(
            builder ->
                builder.index("datasetindex_v3_1").acknowledged(true).shardsAcknowledged(true));
    when(mockIndices.create(any(CreateIndexRequest.class))).thenReturn(fakeResponse);

    org.opensearch.client.indices.CreateIndexRequest request =
        new org.opensearch.client.indices.CreateIndexRequest("datasetindex_v3_1");
    request.alias(new Alias("datasetindex_v3"));

    Es8SearchClientShim.forTest(mockClient)
        .createIndex(OperationFingerprint.EMPTY, request, RequestOptions.DEFAULT);

    ArgumentCaptor<CreateIndexRequest> captor = ArgumentCaptor.forClass(CreateIndexRequest.class);
    verify(mockIndices).create(captor.capture());
    assertEquals(captor.getValue().index(), "datasetindex_v3_1");
    assertTrue(captor.getValue().aliases().containsKey("datasetindex_v3"));
  }
}
