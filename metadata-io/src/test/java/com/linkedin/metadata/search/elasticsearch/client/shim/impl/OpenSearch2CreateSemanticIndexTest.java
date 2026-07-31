package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.utils.elasticsearch.shim.SemanticIndexSpec;
import java.io.IOException;
import org.mockito.ArgumentCaptor;
import org.opensearch.client.IndicesClient;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.testng.annotations.Test;

public class OpenSearch2CreateSemanticIndexTest {

  private static SemanticIndexSpec testSpec() {
    return SemanticIndexSpec.builder()
        .indexName("dataset_semantic_v1")
        .modelKey("text_embedding_3_large")
        .vectorDimension(768)
        .build();
  }

  @Test
  public void createSemanticIndexSendsCorrectIndexName() throws IOException {
    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);
    IndicesClient mockIndices = mock(IndicesClient.class);
    when(mockClient.indices()).thenReturn(mockIndices);

    CreateIndexResponse fakeResponse = mock(CreateIndexResponse.class);
    when(fakeResponse.isAcknowledged()).thenReturn(true);
    when(mockIndices.create(any(CreateIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(fakeResponse);

    OpenSearch2SearchClientShim shim = OpenSearch2SearchClientShim.forTest(mockClient);
    shim.createSemanticIndex(testSpec());

    ArgumentCaptor<CreateIndexRequest> captor = ArgumentCaptor.forClass(CreateIndexRequest.class);
    verify(mockIndices).create(captor.capture(), eq(RequestOptions.DEFAULT));
    assertEquals(captor.getValue().index(), "dataset_semantic_v1");
  }

  @Test(expectedExceptions = IOException.class)
  public void createSemanticIndexThrowsWhenNotAcknowledged() throws IOException {
    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);
    IndicesClient mockIndices = mock(IndicesClient.class);
    when(mockClient.indices()).thenReturn(mockIndices);

    CreateIndexResponse nackResponse = mock(CreateIndexResponse.class);
    when(nackResponse.isAcknowledged()).thenReturn(false);
    when(mockIndices.create(any(CreateIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(nackResponse);

    OpenSearch2SearchClientShim shim = OpenSearch2SearchClientShim.forTest(mockClient);
    shim.createSemanticIndex(testSpec());
  }
}
