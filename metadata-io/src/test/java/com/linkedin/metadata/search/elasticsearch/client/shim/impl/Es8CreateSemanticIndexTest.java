package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import com.linkedin.metadata.utils.elasticsearch.shim.SemanticIndexSpec;
import java.io.IOException;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class Es8CreateSemanticIndexTest {

  private static SemanticIndexSpec testSpec() {
    return SemanticIndexSpec.builder()
        .indexName("dataset_semantic_v1")
        .modelKey("gemini_embedding_001")
        .vectorDimension(768)
        .build();
  }

  @Test
  public void createSemanticIndexSendsCorrectIndexName() throws IOException {
    ElasticsearchClient mockClient = mock(ElasticsearchClient.class);
    ElasticsearchIndicesClient mockIndices = mock(ElasticsearchIndicesClient.class);
    when(mockClient.indices()).thenReturn(mockIndices);

    co.elastic.clients.elasticsearch.indices.CreateIndexResponse fakeResponse =
        co.elastic.clients.elasticsearch.indices.CreateIndexResponse.of(
            b -> b.index("dataset_semantic_v1").acknowledged(true).shardsAcknowledged(true));
    when(mockIndices.create(any(CreateIndexRequest.class))).thenReturn(fakeResponse);

    Es8SearchClientShim shim = Es8SearchClientShim.forTest(mockClient);
    shim.createSemanticIndex(testSpec());

    ArgumentCaptor<CreateIndexRequest> captor = ArgumentCaptor.forClass(CreateIndexRequest.class);
    verify(mockIndices).create(captor.capture());
    assertEquals(captor.getValue().index(), "dataset_semantic_v1");
  }

  @Test(expectedExceptions = IOException.class)
  public void createSemanticIndexThrowsWhenNotAcknowledged() throws IOException {
    ElasticsearchClient mockClient = mock(ElasticsearchClient.class);
    ElasticsearchIndicesClient mockIndices = mock(ElasticsearchIndicesClient.class);
    when(mockClient.indices()).thenReturn(mockIndices);

    co.elastic.clients.elasticsearch.indices.CreateIndexResponse nackResponse =
        co.elastic.clients.elasticsearch.indices.CreateIndexResponse.of(
            b -> b.index("dataset_semantic_v1").acknowledged(false).shardsAcknowledged(false));
    when(mockIndices.create(any(CreateIndexRequest.class))).thenReturn(nackResponse);

    Es8SearchClientShim shim = Es8SearchClientShim.forTest(mockClient);
    shim.createSemanticIndex(testSpec());
  }
}
