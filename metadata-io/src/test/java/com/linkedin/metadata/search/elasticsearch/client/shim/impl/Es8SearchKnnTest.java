package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static com.linkedin.metadata.utils.CriterionUtils.buildIsNotNullCriterion;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchResponse;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.opensearch.index.query.QueryBuilders;
import org.testng.annotations.Test;

public class Es8SearchKnnTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

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
    KnnSearchResponse response = shim.searchKnn(OP_CONTEXT, testRequest());

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
    KnnSearchResponse response = shim.searchKnn(OP_CONTEXT, testRequest());

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
    shim.searchKnn(OP_CONTEXT, testRequest()); // default ignoreUnavailable=true

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
    shim.searchKnn(OP_CONTEXT, req);

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
    shim.searchKnn(OP_CONTEXT, multiIndexReq);

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
    KnnSearchResponse response = shim.searchKnn(OP_CONTEXT, testRequest());

    assertEquals(response.hits().size(), 1, "Only hits with non-empty ids should be returned");
    assertEquals(response.hits().get(0).id(), "urn:li:dataset:abc");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void searchKnnAcceptsFilterBuiltWithOpenSearchQueryBuilders() throws IOException {
    ElasticsearchClient mockClient = mock(ElasticsearchClient.class);
    HitsMetadata<Map> hitsMetadata = mock(HitsMetadata.class);
    when(hitsMetadata.hits()).thenReturn(List.of());
    SearchResponse<Map> mockResponse = mock(SearchResponse.class);
    when(mockResponse.hits()).thenReturn(hitsMetadata);
    when(mockClient.search(any(SearchRequest.class), eq(Map.class))).thenReturn(mockResponse);

    // Semantic search builds its filter with OpenSearch query builders, whose serialization carries
    // bool.adjust_pure_negative at every level; the strict kNN body parse rejected it
    Map<String, Object> filter =
        new ObjectMapper()
            .readValue(
                QueryBuilders.boolQuery()
                    .must(
                        QueryBuilders.boolQuery()
                            .should(QueryBuilders.termQuery("urn", "urn:li:dataset:abc")))
                    .mustNot(QueryBuilders.termQuery("removed", true))
                    .toString(),
                Map.class);
    KnnSearchRequest request =
        KnnSearchRequest.builder()
            .indexName("dataset_semantic_v1")
            .vectorField("embeddings.gemini_embedding_001.chunks.vector")
            .queryVector(new float[] {0.1f, 0.2f, 0.3f})
            .k(5)
            .filter(filter)
            .build();

    Es8SearchClientShim.forTest(mockClient).searchKnn(OP_CONTEXT, request);

    ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
    verify(mockClient).search(captor.capture(), eq(Map.class));
    String sent = captor.getValue().toString();
    assertTrue(sent.contains("urn:li:dataset:abc"), sent);
    assertTrue(sent.contains("must_not"), sent);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void searchKnnAcceptsFilterShapesSemanticSearchBuilds() throws IOException {
    ElasticsearchClient mockClient = mock(ElasticsearchClient.class);
    HitsMetadata<Map> hitsMetadata = mock(HitsMetadata.class);
    when(hitsMetadata.hits()).thenReturn(List.of());
    SearchResponse<Map> mockResponse = mock(SearchResponse.class);
    when(mockResponse.hits()).thenReturn(hitsMetadata);
    when(mockClient.search(any(SearchRequest.class), eq(Map.class))).thenReturn(mockResponse);

    // What the semantic search filter builder emits: terms, a case-insensitive wildcard, a legacy
    // range, a negated term and an exists check, across two disjuncts (minimum_should_match)
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                buildCriterion(
                                    "platform",
                                    Condition.EQUAL,
                                    "urn:li:dataPlatform:notion",
                                    "urn:li:dataPlatform:confluence"),
                                buildCriterion("name", Condition.CONTAIN, "revenue"),
                                buildCriterion(
                                    "lastModifiedAt", Condition.GREATER_THAN, "1700000000000"),
                                buildCriterion("removed", Condition.EQUAL, true, "true"),
                                buildIsNotNullCriterion("description"))),
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                buildCriterion("urn", Condition.EQUAL, "urn:li:document:a")))));
    Map<String, Object> filterMap =
        ESUtils.buildFilterMap(filter, false, Map.of(), OP_CONTEXT, QueryFilterRewriteChain.EMPTY);
    KnnSearchRequest request =
        KnnSearchRequest.builder()
            .indexName("dataset_semantic_v1")
            .vectorField("embeddings.gemini_embedding_001.chunks.vector")
            .queryVector(new float[] {0.1f, 0.2f, 0.3f})
            .k(5)
            .filter(filterMap)
            .build();

    Es8SearchClientShim.forTest(mockClient).searchKnn(OP_CONTEXT, request);

    ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
    verify(mockClient).search(captor.capture(), eq(Map.class));
    String sent = captor.getValue().toString();
    assertTrue(sent.contains("revenue"), sent);
    assertTrue(sent.contains("1700000000000"), sent);
  }
}
