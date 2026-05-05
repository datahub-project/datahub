package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.utils.elasticsearch.shim.EmbeddingBatch;
import java.io.IOException;
import java.util.List;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.testng.annotations.Test;

public class OpenSearch2IndexEmbeddingsTest {

  private static EmbeddingBatch testBatch() {
    EmbeddingBatch.Chunk chunk =
        new EmbeddingBatch.Chunk(new float[] {0.1f, 0.2f}, "some text", 0, 0, 9, 2);
    return new EmbeddingBatch(
        "dataset_semantic_v1", "urn:li:dataset:test", "text_embedding_3_large", List.of(chunk));
  }

  @Test
  public void indexEmbeddingsSendsCorrectIndexAndDocumentId() throws IOException {
    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);
    IndexResponse mockResponse = mock(IndexResponse.class);
    when(mockResponse.getResult()).thenReturn(DocWriteResponse.Result.CREATED);
    when(mockClient.index(any(IndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockResponse);

    OpenSearch2SearchClientShim shim = OpenSearch2SearchClientShim.forTest(mockClient);
    shim.indexEmbeddings(testBatch());

    ArgumentCaptor<IndexRequest> captor = ArgumentCaptor.forClass(IndexRequest.class);
    verify(mockClient).index(captor.capture(), eq(RequestOptions.DEFAULT));
    assertEquals(captor.getValue().index(), "dataset_semantic_v1");
    assertEquals(captor.getValue().id(), "urn:li:dataset:test");
  }

  @Test(expectedExceptions = IOException.class)
  public void indexEmbeddingsThrowsOnUnexpectedResult() throws IOException {
    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);
    IndexResponse mockResponse = mock(IndexResponse.class);
    when(mockResponse.getResult()).thenReturn(DocWriteResponse.Result.NOOP);
    when(mockClient.index(any(IndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockResponse);

    OpenSearch2SearchClientShim shim = OpenSearch2SearchClientShim.forTest(mockClient);
    shim.indexEmbeddings(testBatch());
  }
}
