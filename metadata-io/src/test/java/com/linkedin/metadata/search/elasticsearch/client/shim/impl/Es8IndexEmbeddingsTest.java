package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import com.linkedin.metadata.utils.elasticsearch.shim.EmbeddingBatch;
import java.io.IOException;
import java.util.List;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class Es8IndexEmbeddingsTest {

  private static EmbeddingBatch testBatch() {
    EmbeddingBatch.Chunk chunk =
        new EmbeddingBatch.Chunk(new float[] {0.1f, 0.2f}, "some text", 0, 0, 9, 2);
    return new EmbeddingBatch(
        "dataset_semantic_v1", "urn:li:dataset:test", "gemini_embedding_001", List.of(chunk));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void indexEmbeddingsSendsCorrectIndexAndDocumentId() throws IOException {
    ElasticsearchClient mockClient = mock(ElasticsearchClient.class);
    IndexResponse mockResponse = mock(IndexResponse.class);
    when(mockResponse.result()).thenReturn(Result.Created);
    when(mockClient.index(any(IndexRequest.class))).thenReturn(mockResponse);

    Es8SearchClientShim shim = Es8SearchClientShim.forTest(mockClient);
    shim.indexEmbeddings(testBatch());

    ArgumentCaptor<IndexRequest> captor = ArgumentCaptor.forClass(IndexRequest.class);
    verify(mockClient).index(captor.capture());
    assertEquals(captor.getValue().index(), "dataset_semantic_v1");
    assertEquals(captor.getValue().id(), "urn:li:dataset:test");
  }

  @Test(expectedExceptions = IOException.class)
  @SuppressWarnings("unchecked")
  public void indexEmbeddingsThrowsOnUnexpectedResult() throws IOException {
    ElasticsearchClient mockClient = mock(ElasticsearchClient.class);
    IndexResponse mockResponse = mock(IndexResponse.class);
    when(mockResponse.result()).thenReturn(Result.NoOp);
    when(mockClient.index(any(IndexRequest.class))).thenReturn(mockResponse);

    Es8SearchClientShim shim = Es8SearchClientShim.forTest(mockClient);
    shim.indexEmbeddings(testBatch());
  }
}
