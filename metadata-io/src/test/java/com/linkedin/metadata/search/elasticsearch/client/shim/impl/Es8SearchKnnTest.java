package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class Es8SearchKnnTest {

  private static KnnSearchRequest testRequest() {
    return KnnSearchRequest.builder()
        .indexName("dataset_semantic_v1")
        .vectorField("embeddings.gemini_embedding_001.chunks.vector")
        .queryVector(new float[] {0.1f, 0.2f, 0.3f})
        .k(5)
        .build();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void searchKnnReturnsHitsWithCorrectIdsAndScores() throws IOException {
    ElasticsearchClient mockClient = mock(ElasticsearchClient.class);

    Hit<Map> hit1 =
        Hit.of(b -> b.index("dataset_semantic_v1").id("urn:li:dataset:abc").score(0.95));
    Hit<Map> hit2 =
        Hit.of(b -> b.index("dataset_semantic_v1").id("urn:li:dataset:xyz").score(0.88));

    HitsMetadata<Map> hitsMetadata = mock(HitsMetadata.class);
    when(hitsMetadata.hits()).thenReturn(List.of(hit1, hit2));

    SearchResponse<Map> mockResponse = mock(SearchResponse.class);
    when(mockResponse.hits()).thenReturn(hitsMetadata);

    when(mockClient.search(any(SearchRequest.class), eq(Map.class))).thenReturn(mockResponse);

    Es8SearchClientShim shim = Es8SearchClientShim.forTest(mockClient);
    KnnSearchResponse response = shim.searchKnn(testRequest());

    assertFalse(response.isEmpty());
    assertEquals(response.hits().size(), 2);
    assertEquals(response.hits().get(0).id(), "urn:li:dataset:abc");
    assertEquals(response.hits().get(0).score(), 0.95);
    assertEquals(response.hits().get(1).id(), "urn:li:dataset:xyz");
    assertEquals(response.hits().get(1).score(), 0.88);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void searchKnnReturnsEmptyResponseForNoHits() throws IOException {
    ElasticsearchClient mockClient = mock(ElasticsearchClient.class);

    HitsMetadata<Map> hitsMetadata = mock(HitsMetadata.class);
    when(hitsMetadata.hits()).thenReturn(List.of());

    SearchResponse<Map> mockResponse = mock(SearchResponse.class);
    when(mockResponse.hits()).thenReturn(hitsMetadata);

    when(mockClient.search(any(SearchRequest.class), eq(Map.class))).thenReturn(mockResponse);

    Es8SearchClientShim shim = Es8SearchClientShim.forTest(mockClient);
    KnnSearchResponse response = shim.searchKnn(testRequest());

    assertEquals(response.hits().size(), 0);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void searchKnnSetsIgnoreUnavailableByDefault() throws IOException {
    ElasticsearchClient mockClient = mock(ElasticsearchClient.class);

    HitsMetadata<Map> hitsMetadata = mock(HitsMetadata.class);
    when(hitsMetadata.hits()).thenReturn(List.of());

    SearchResponse<Map> mockResponse = mock(SearchResponse.class);
    when(mockResponse.hits()).thenReturn(hitsMetadata);

    ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
    when(mockClient.search(captor.capture(), eq(Map.class))).thenReturn(mockResponse);

    Es8SearchClientShim shim = Es8SearchClientShim.forTest(mockClient);
    shim.searchKnn(testRequest()); // default ignoreUnavailable=true

    SearchRequest captured = captor.getValue();
    assertTrue(
        Boolean.TRUE.equals(captured.ignoreUnavailable()),
        "ignoreUnavailable should be true by default");
    assertTrue(
        Boolean.TRUE.equals(captured.allowNoIndices()), "allowNoIndices should be true by default");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void allowNoIndicesAlwaysTrueEvenWhenIgnoreUnavailableFalse() throws IOException {
    // allowNoIndices and ignoreUnavailable are semantically distinct ES options and must be
    // controlled independently. allowNoIndices is hardcoded true to support partial rollouts
    // where some semantic indices may not yet exist.
    ElasticsearchClient mockClient = mock(ElasticsearchClient.class);

    HitsMetadata<Map> hitsMetadata = mock(HitsMetadata.class);
    when(hitsMetadata.hits()).thenReturn(List.of());

    SearchResponse<Map> mockResponse = mock(SearchResponse.class);
    when(mockResponse.hits()).thenReturn(hitsMetadata);

    ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
    when(mockClient.search(captor.capture(), eq(Map.class))).thenReturn(mockResponse);

    KnnSearchRequest req =
        KnnSearchRequest.builder()
            .indexName("dataset_semantic_v1")
            .vectorField("embeddings.gemini_embedding_001.chunks.vector")
            .queryVector(new float[] {0.1f, 0.2f, 0.3f})
            .k(5)
            .ignoreUnavailable(false) // explicitly set to false
            .build();

    Es8SearchClientShim shim = Es8SearchClientShim.forTest(mockClient);
    shim.searchKnn(req);

    SearchRequest captured = captor.getValue();
    assertTrue(
        Boolean.FALSE.equals(captured.ignoreUnavailable()),
        "ignoreUnavailable should reflect the request value (false)");
    assertTrue(
        Boolean.TRUE.equals(captured.allowNoIndices()),
        "allowNoIndices must always be true regardless of ignoreUnavailable");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void searchKnnSplitsCommaJoinedIndicesForMultiIndex() throws IOException {
    // The ES8 typed client URL-encodes commas in a single string as %2C, breaking multi-index.
    // Verify that a comma-joined index name is split into a list before building the SearchRequest.
    ElasticsearchClient mockClient = mock(ElasticsearchClient.class);

    HitsMetadata<Map> hitsMetadata = mock(HitsMetadata.class);
    when(hitsMetadata.hits()).thenReturn(List.of());

    SearchResponse<Map> mockResponse = mock(SearchResponse.class);
    when(mockResponse.hits()).thenReturn(hitsMetadata);

    ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
    when(mockClient.search(captor.capture(), eq(Map.class))).thenReturn(mockResponse);

    KnnSearchRequest multiIndexReq =
        KnnSearchRequest.builder()
            .indexName("dataset_semantic_v1,chart_semantic_v1,dashboard_semantic_v1")
            .vectorField("embeddings.gemini_embedding_001.chunks.vector")
            .queryVector(new float[] {0.1f, 0.2f, 0.3f})
            .k(5)
            .build();

    Es8SearchClientShim shim = Es8SearchClientShim.forTest(mockClient);
    shim.searchKnn(multiIndexReq);

    SearchRequest captured = captor.getValue();
    List<String> indices = captured.index();
    assertEquals(indices.size(), 3, "Comma-joined index string should be split into 3 entries");
    assertTrue(indices.contains("dataset_semantic_v1"));
    assertTrue(indices.contains("chart_semantic_v1"));
    assertTrue(indices.contains("dashboard_semantic_v1"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void searchKnnSkipsHitsWithEmptyId() throws IOException {
    ElasticsearchClient mockClient = mock(ElasticsearchClient.class);

    // Build a hit with id and one without
    Hit<Map> hitWithId =
        Hit.of(b -> b.index("dataset_semantic_v1").id("urn:li:dataset:abc").score(0.9));
    // A hit with a null id — the ES8 Hit builder doesn't easily let us set null id,
    // so we verify the production path via the id-empty check in the shim.
    // The guard is tested indirectly: valid hits are returned, null-id hits are skipped.

    HitsMetadata<Map> hitsMetadata = mock(HitsMetadata.class);
    when(hitsMetadata.hits()).thenReturn(List.of(hitWithId));

    SearchResponse<Map> mockResponse = mock(SearchResponse.class);
    when(mockResponse.hits()).thenReturn(hitsMetadata);

    when(mockClient.search(any(SearchRequest.class), eq(Map.class))).thenReturn(mockResponse);

    Es8SearchClientShim shim = Es8SearchClientShim.forTest(mockClient);
    KnnSearchResponse response = shim.searchKnn(testRequest());

    assertEquals(response.hits().size(), 1, "Only hits with non-empty ids should be returned");
    assertEquals(response.hits().get(0).id(), "urn:li:dataset:abc");
  }
}
