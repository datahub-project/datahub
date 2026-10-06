package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.SearchEngineType;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.ShimConfiguration;
import com.linkedin.metadata.utils.elasticsearch.shim.EmbeddingBatch;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchResponse;
import com.linkedin.metadata.utils.elasticsearch.shim.SemanticIndexSpec;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.http.ProtocolVersion;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.util.EntityUtils;
import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.explain.ExplainRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.ResizeRequest;
import org.opensearch.client.tasks.GetTaskRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Unit tests for {@link OpenSearchSearchClientShim} request execution and response parsing over a
 * mocked low-level {@link RestClient} — no live cluster required. Wire-level behavior against real
 * OpenSearch 2.x/3.x is covered by the testOpenSearch / testOpenSearch3 integration suites.
 */
public class OpenSearchSearchClientShimTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final OperationFingerprint OP = OperationFingerprint.EMPTY;

  private static Response jsonResponse(int statusCode, String json) {
    Response response = mock(Response.class);
    StatusLine statusLine =
        new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), statusCode, null);
    when(response.getStatusLine()).thenReturn(statusLine);
    when(response.getEntity()).thenReturn(new StringEntity(json, ContentType.APPLICATION_JSON));
    return response;
  }

  private static ResponseException responseException(int statusCode, String json)
      throws IOException {
    Response response = jsonResponse(statusCode, json);
    RequestLine requestLine = mock(RequestLine.class);
    when(requestLine.getMethod()).thenReturn("GET");
    when(requestLine.getUri()).thenReturn("/test");
    when(response.getRequestLine()).thenReturn(requestLine);
    return new ResponseException(response);
  }

  private static OpenSearchSearchClientShim shimWith(RestClient restClient) {
    return OpenSearchSearchClientShim.forTest(restClient);
  }

  @Test
  public void searchParsesResponseThroughServerParsers() throws Exception {
    RestClient restClient = mock(RestClient.class);
    String body =
        "{\"took\":3,\"timed_out\":false,"
            + "\"_shards\":{\"total\":1,\"successful\":1,\"skipped\":0,\"failed\":0},"
            + "\"hits\":{\"total\":{\"value\":2,\"relation\":\"eq\"},\"max_score\":1.0,"
            + "\"hits\":[{\"_index\":\"idx\",\"_id\":\"doc1\",\"_score\":1.0,\"_source\":{}},"
            + "{\"_index\":\"idx\",\"_id\":\"doc2\",\"_score\":0.5,\"_source\":{}}]}}";
    Response ok = jsonResponse(200, body);
    when(restClient.performRequest(any(Request.class))).thenReturn(ok);

    SearchRequest searchRequest = new SearchRequest("idx");
    searchRequest.source(new SearchSourceBuilder());
    SearchResponse response =
        shimWith(restClient).search(OP, searchRequest, RequestOptions.DEFAULT);

    assertEquals(response.getHits().getTotalHits().value, 2L);
    assertEquals(response.getHits().getHits()[0].getId(), "doc1");
  }

  @Test
  public void getDocumentParses404BodyAsMissingDocument() throws Exception {
    RestClient restClient = mock(RestClient.class);
    String notFoundBody = "{\"_index\":\"idx\",\"_id\":\"missing\",\"found\":false}";
    ResponseException re404 = responseException(404, notFoundBody);
    when(restClient.performRequest(any(Request.class))).thenThrow(re404);

    GetResponse response =
        shimWith(restClient)
            .getDocument(OP, new GetRequest("idx", "missing"), RequestOptions.DEFAULT);

    assertFalse(response.isExists());
    assertEquals(response.getId(), "missing");
  }

  @Test
  public void indexExistsMapsStatusCodes() throws Exception {
    RestClient existing = mock(RestClient.class);
    Response existsOk = jsonResponse(200, "");
    when(existing.performRequest(any(Request.class))).thenReturn(existsOk);
    assertTrue(
        shimWith(existing).indexExists(OP, new GetIndexRequest("idx"), RequestOptions.DEFAULT));

    RestClient missing = mock(RestClient.class);
    ResponseException re404 = responseException(404, "{}");
    when(missing.performRequest(any(Request.class))).thenThrow(re404);
    assertFalse(
        shimWith(missing).indexExists(OP, new GetIndexRequest("idx"), RequestOptions.DEFAULT));
  }

  @Test
  public void errorsTranslateToOpenSearchStatusExceptionWithParsedReason() throws Exception {
    RestClient restClient = mock(RestClient.class);
    String errorBody =
        "{\"error\":{\"type\":\"resource_already_exists_exception\","
            + "\"reason\":\"index [idx] already exists\",\"root_cause\":["
            + "{\"type\":\"resource_already_exists_exception\","
            + "\"reason\":\"index [idx] already exists\"}]},\"status\":400}";
    ResponseException re400 = responseException(400, errorBody);
    when(restClient.performRequest(any(Request.class))).thenThrow(re400);

    SearchRequest searchRequest = new SearchRequest("idx");
    searchRequest.source(new SearchSourceBuilder());
    OpenSearchStatusException e =
        org.testng.Assert.expectThrows(
            OpenSearchStatusException.class,
            () -> shimWith(restClient).search(OP, searchRequest, RequestOptions.DEFAULT));
    assertEquals(e.status(), RestStatus.BAD_REQUEST);
    assertTrue(e.getMessage().contains("resource_already_exists_exception"));
  }

  @Test
  public void errorsWithUnparseableBodyStillCarryStatus() throws Exception {
    RestClient restClient = mock(RestClient.class);
    ResponseException re503 = responseException(503, "upstream unavailable");
    when(restClient.performRequest(any(Request.class))).thenThrow(re503);

    SearchRequest searchRequest = new SearchRequest("idx");
    searchRequest.source(new SearchSourceBuilder());
    OpenSearchStatusException e =
        org.testng.Assert.expectThrows(
            OpenSearchStatusException.class,
            () -> shimWith(restClient).search(OP, searchRequest, RequestOptions.DEFAULT));
    assertEquals(e.status(), RestStatus.SERVICE_UNAVAILABLE);
  }

  @Test
  public void getTaskReturnsEmptyOn404() throws Exception {
    RestClient restClient = mock(RestClient.class);
    ResponseException re404 = responseException(404, "{}");
    when(restClient.performRequest(any(Request.class))).thenThrow(re404);

    Optional<?> task =
        shimWith(restClient).getTask(new GetTaskRequest("node", 42L), RequestOptions.DEFAULT);
    assertTrue(task.isEmpty());
  }

  @Test
  public void getClusterInfoExposesVersionAndEngineType() throws Exception {
    RestClient restClient = mock(RestClient.class);
    String body =
        "{\"name\":\"node1\",\"cluster_name\":\"test-cluster\",\"cluster_uuid\":\"uuid1\","
            + "\"version\":{\"distribution\":\"opensearch\",\"number\":\"3.2.0\","
            + "\"build_type\":\"tar\",\"build_hash\":\"abc\",\"build_date\":\"2026-01-01\","
            + "\"build_snapshot\":false,\"lucene_version\":\"10.0.0\","
            + "\"minimum_wire_compatibility_version\":\"2.19.0\","
            + "\"minimum_index_compatibility_version\":\"2.0.0\"},"
            + "\"tagline\":\"The OpenSearch Project: https://opensearch.org/\"}";
    Response ok = jsonResponse(200, body);
    when(restClient.performRequest(any(Request.class))).thenReturn(ok);

    OpenSearchSearchClientShim shim = shimWith(restClient);
    Map<String, String> clusterInfo = shim.getClusterInfo();
    assertEquals(clusterInfo.get("version"), "3.2.0");
    assertEquals(clusterInfo.get("engine_type"), "opensearch");
    assertEquals(shim.getEngineVersion(), "3.2.0");
  }

  @Test
  public void engineTypeComesFromConfiguration() throws Exception {
    ShimConfiguration os3Config = mock(ShimConfiguration.class);
    when(os3Config.getEngineType()).thenReturn(SearchEngineType.OPENSEARCH_3);
    OpenSearchSearchClientShim shim =
        OpenSearchSearchClientShim.forTest(mock(RestClient.class), os3Config);
    assertEquals(shim.getEngineType(), SearchEngineType.OPENSEARCH_3);
    assertTrue(shim.getEngineType().requiresOpenSearchClient());

    ShimConfiguration os2Config = mock(ShimConfiguration.class);
    when(os2Config.getEngineType()).thenReturn(SearchEngineType.OPENSEARCH_2);
    OpenSearchSearchClientShim os2Shim =
        OpenSearchSearchClientShim.forTest(mock(RestClient.class), os2Config);
    assertEquals(os2Shim.getEngineType(), SearchEngineType.OPENSEARCH_2);
  }

  @Test
  public void parseSearchKnnResponseParsesHits() throws Exception {
    String responseJson =
        "{\"hits\":{\"hits\":["
            + "{\"_id\":\"urn:li:dataset:abc\",\"_score\":0.95,\"_source\":{\"urn\":\"urn:li:dataset:abc\"}},"
            + "{\"_id\":\"\",\"_score\":0.5}"
            + "]}}";
    var response =
        OpenSearchSearchClientShim.parseSearchKnnResponse(MAPPER.readTree(responseJson), MAPPER);
    assertEquals(response.hits().size(), 1);
    assertEquals(response.hits().get(0).id(), "urn:li:dataset:abc");
    assertEquals(response.hits().get(0).score(), 0.95, 0.001);
  }

  @Test
  public void semanticVersionGateAcceptsOpenSearch3() {
    OpenSearchSearchClientShim.assertSemanticSearchSupported("3.2.0", true);
    OpenSearchSearchClientShim.assertSemanticSearchSupported("2.19.1", true);
    assertThrows(
        IllegalStateException.class,
        () -> OpenSearchSearchClientShim.assertSemanticSearchSupported("2.17.0", true));
    assertNotNull(OpenSearchSearchClientShim.PARTIAL_NGRAM_CONFIG.get("type"));
  }

  @Test
  public void semanticVersionGateRejectsUnparseableAndPrereleaseVersions() {
    OpenSearchSearchClientShim.assertSemanticSearchSupported("2.17.0");
    assertThrows(
        IllegalStateException.class,
        () -> OpenSearchSearchClientShim.assertSemanticSearchSupported("not-a-version"));
    assertThrows(
        IllegalStateException.class,
        () -> OpenSearchSearchClientShim.assertSemanticSearchSupported(null));
    assertThrows(
        IllegalStateException.class,
        () -> OpenSearchSearchClientShim.assertSemanticSearchSupported("2.16.9"));
    // 2.19.0 prereleases predate faiss cosine support even though they parse as 2.19.
    assertThrows(
        IllegalStateException.class,
        () -> OpenSearchSearchClientShim.assertSemanticSearchSupported("2.19.0-alpha1", true));
  }

  @Test
  public void verifySemanticSearchSupportSkipsWhenVersionUnreadable() throws Exception {
    RestClient restClient = mock(RestClient.class);
    when(restClient.performRequest(any(Request.class)))
        .thenThrow(new IOException("cluster info restricted"));

    // An unreadable version is indeterminate, not proof of an unsupported cluster.
    shimWith(restClient).verifySemanticSearchSupport();
    shimWith(restClient).verifySemanticSearchSupport(true);
  }

  @Test
  public void verifySemanticSearchSupportUsesLiveClusterVersion() throws Exception {
    RestClient restClient = mock(RestClient.class);
    String body =
        "{\"name\":\"node1\",\"cluster_name\":\"test-cluster\",\"cluster_uuid\":\"uuid1\","
            + "\"version\":{\"distribution\":\"opensearch\",\"number\":\"2.9.0\","
            + "\"build_type\":\"tar\",\"build_hash\":\"abc\",\"build_date\":\"2026-01-01\","
            + "\"build_snapshot\":false,\"lucene_version\":\"9.7.0\","
            + "\"minimum_wire_compatibility_version\":\"2.0.0\","
            + "\"minimum_index_compatibility_version\":\"2.0.0\"},"
            + "\"tagline\":\"The OpenSearch Project: https://opensearch.org/\"}";
    Response ok = jsonResponse(200, body);
    when(restClient.performRequest(any(Request.class))).thenReturn(ok);

    assertThrows(
        IllegalStateException.class, () -> shimWith(restClient).verifySemanticSearchSupport());
  }

  @Test
  public void getClusterInfoWrapsRestrictedClusterErrors() throws Exception {
    String forbiddenBody =
        "{\"error\":{\"type\":\"security_exception\","
            + "\"reason\":\"no permissions for cluster:monitor/main\"},\"status\":403}";

    // Explicitly configured engine type: restricted cluster info is expected, still an IOException.
    ResponseException forbidden = responseException(403, forbiddenBody);
    ShimConfiguration explicitConfig = mock(ShimConfiguration.class);
    when(explicitConfig.getEngineType()).thenReturn(SearchEngineType.OPENSEARCH_2);
    when(explicitConfig.isEngineTypeAutoDetected()).thenReturn(false);
    RestClient restricted = mock(RestClient.class);
    when(restricted.performRequest(any(Request.class))).thenThrow(forbidden);
    OpenSearchSearchClientShim shim =
        OpenSearchSearchClientShim.forTest(restricted, explicitConfig);
    assertThrows(IOException.class, shim::getClusterInfo);
    assertEquals(shim.getEngineVersion(), "unknown");

    // Auto-detected engine type: the version is required, same IOException via the error branch.
    ShimConfiguration autoConfig = mock(ShimConfiguration.class);
    when(autoConfig.getEngineType()).thenReturn(SearchEngineType.OPENSEARCH_2);
    when(autoConfig.isEngineTypeAutoDetected()).thenReturn(true);
    RestClient restrictedAuto = mock(RestClient.class);
    when(restrictedAuto.performRequest(any(Request.class))).thenThrow(forbidden);
    assertThrows(
        IOException.class,
        () -> OpenSearchSearchClientShim.forTest(restrictedAuto, autoConfig).getClusterInfo());
  }

  @Test
  public void searchKnnBuildsRequestAndParsesHits() throws Exception {
    RestClient restClient = mock(RestClient.class);
    String body =
        "{\"hits\":{\"hits\":["
            // Full hit: source plus nested inner_hits, which the parser tolerates and ignores.
            + "{\"_id\":\"urn:li:dataset:abc\",\"_score\":0.95,"
            + "\"_source\":{\"urn\":\"urn:li:dataset:abc\"},"
            + "\"inner_hits\":{\"chunks\":{\"hits\":{\"hits\":["
            + "{\"_source\":{\"wrapper\":{\"text\":\"chunk text\"}}},"
            + "{\"_source\":{\"noText\":{\"x\":1}}}]}}}},"
            // Hit without an id is skipped.
            + "{\"_score\":0.5,\"_source\":{}},"
            // Null score defaults to 0.0; no source yields an empty map.
            + "{\"_id\":\"urn:li:dataset:xyz\",\"_score\":null}"
            + "]}}";
    Response ok = jsonResponse(200, body);
    when(restClient.performRequest(any(Request.class))).thenReturn(ok);

    KnnSearchRequest request =
        KnnSearchRequest.builder()
            .indexName("dataset_semantic_v1")
            .vectorField("embeddings.model.chunks.vector")
            .queryVector(new float[] {0.1f, 0.2f, 0.3f})
            .k(5)
            .build();
    KnnSearchResponse response = shimWith(restClient).searchKnn(OP, request);

    ArgumentCaptor<Request> sent = ArgumentCaptor.forClass(Request.class);
    org.mockito.Mockito.verify(restClient).performRequest(sent.capture());
    assertEquals(sent.getValue().getEndpoint(), "/dataset_semantic_v1/_search");
    assertEquals(sent.getValue().getParameters().get("allow_no_indices"), "true");
    // KnnSearchRequest defaults ignoreUnavailable to true for partial-rollout resilience.
    assertEquals(sent.getValue().getParameters().get("ignore_unavailable"), "true");

    assertEquals(response.hits().size(), 2);
    assertEquals(response.hits().get(0).id(), "urn:li:dataset:abc");
    assertEquals(response.hits().get(0).score(), 0.95, 0.0001);
    assertEquals(response.hits().get(1).id(), "urn:li:dataset:xyz");
    assertEquals(response.hits().get(1).score(), 0.0, 0.0001);
    assertTrue(response.hits().get(1).source().isEmpty());
  }

  @Test
  public void indexEmbeddingsWritesDocumentShapeAndRequiresWriteResult() throws Exception {
    RestClient restClient = mock(RestClient.class);
    String created =
        "{\"_index\":\"doc_semantic\",\"_id\":\"urn:doc:1\",\"_version\":1,\"result\":\"created\","
            + "\"_shards\":{\"total\":1,\"successful\":1,\"failed\":0},"
            + "\"_seq_no\":0,\"_primary_term\":1}";
    Response createdResponse = jsonResponse(200, created);
    when(restClient.performRequest(any(Request.class))).thenReturn(createdResponse);

    EmbeddingBatch batch =
        new EmbeddingBatch(
            "doc_semantic",
            "urn:doc:1",
            "test_model",
            List.of(new EmbeddingBatch.Chunk(new float[] {0.1f, 0.2f}, "alpha", 0, 0, 5, 1)));
    shimWith(restClient).indexEmbeddings(OP, batch);

    ArgumentCaptor<Request> sent = ArgumentCaptor.forClass(Request.class);
    org.mockito.Mockito.verify(restClient).performRequest(sent.capture());
    String requestBody = EntityUtils.toString(sent.getValue().getEntity());
    assertTrue(requestBody.contains("\"urn\":\"urn:doc:1\""));
    assertTrue(requestBody.contains("\"test_model\""));
    assertTrue(requestBody.contains("\"text\":\"alpha\""));

    // Anything other than CREATED/UPDATED means the write did not land.
    RestClient noopClient = mock(RestClient.class);
    String noop = created.replace("\"result\":\"created\"", "\"result\":\"noop\"");
    Response noopResponse = jsonResponse(200, noop);
    when(noopClient.performRequest(any(Request.class))).thenReturn(noopResponse);
    assertThrows(IOException.class, () -> shimWith(noopClient).indexEmbeddings(OP, batch));
  }

  @Test
  public void createSemanticIndexRequiresAcknowledgement() throws Exception {
    SemanticIndexSpec spec =
        SemanticIndexSpec.builder()
            .indexName("doc_semantic")
            .modelKey("test_model")
            .vectorDimension(4)
            .build();

    RestClient acknowledged = mock(RestClient.class);
    Response ackResponse =
        jsonResponse(
            200, "{\"acknowledged\":true,\"shards_acknowledged\":true,\"index\":\"doc_semantic\"}");
    when(acknowledged.performRequest(any(Request.class))).thenReturn(ackResponse);
    shimWith(acknowledged).createSemanticIndex(spec);

    RestClient unacknowledged = mock(RestClient.class);
    Response nackResponse =
        jsonResponse(
            200,
            "{\"acknowledged\":false,\"shards_acknowledged\":false,\"index\":\"doc_semantic\"}");
    when(unacknowledged.performRequest(any(Request.class))).thenReturn(nackResponse);
    IOException e =
        org.testng.Assert.expectThrows(
            IOException.class, () -> shimWith(unacknowledged).createSemanticIndex(spec));
    assertTrue(e.getMessage().contains("doc_semantic"));
  }

  @Test
  public void explainParses404AsMissingDocument() throws Exception {
    RestClient restClient = mock(RestClient.class);
    ResponseException re404 =
        responseException(404, "{\"_index\":\"idx\",\"_id\":\"doc1\",\"matched\":false}");
    when(restClient.performRequest(any(Request.class))).thenThrow(re404);

    ExplainRequest explainRequest = new ExplainRequest("idx", "doc1");
    explainRequest.query(QueryBuilders.matchAllQuery());
    var response = shimWith(restClient).explain(OP, explainRequest, RequestOptions.DEFAULT);
    assertFalse(response.isExists());
    assertFalse(response.isMatch());
  }

  @Test
  public void deleteDocumentParsesDeletedAndMissing() throws Exception {
    RestClient deleted = mock(RestClient.class);
    String deletedBody =
        "{\"_index\":\"idx\",\"_id\":\"doc1\",\"_version\":2,\"result\":\"deleted\","
            + "\"_shards\":{\"total\":1,\"successful\":1,\"failed\":0},"
            + "\"_seq_no\":5,\"_primary_term\":1}";
    Response deletedResponse = jsonResponse(200, deletedBody);
    when(deleted.performRequest(any(Request.class))).thenReturn(deletedResponse);
    assertEquals(
        shimWith(deleted)
            .deleteDocument(OP, new DeleteRequest("idx", "doc1"), RequestOptions.DEFAULT)
            .getResult(),
        DocWriteResponse.Result.DELETED);

    // 404 carries a parseable not_found body, matching RHLC's allowed-status behavior.
    RestClient missing = mock(RestClient.class);
    String notFoundBody = deletedBody.replace("\"result\":\"deleted\"", "\"result\":\"not_found\"");
    ResponseException notFound = responseException(404, notFoundBody);
    when(missing.performRequest(any(Request.class))).thenThrow(notFound);
    assertEquals(
        shimWith(missing)
            .deleteDocument(OP, new DeleteRequest("idx", "doc1"), RequestOptions.DEFAULT)
            .getResult(),
        DocWriteResponse.Result.NOT_FOUND);
  }

  @Test
  public void scrollAndClearScrollRoundTrip() throws Exception {
    RestClient restClient = mock(RestClient.class);
    String scrollBody =
        "{\"_scroll_id\":\"scroll-abc\",\"took\":1,\"timed_out\":false,"
            + "\"_shards\":{\"total\":1,\"successful\":1,\"skipped\":0,\"failed\":0},"
            + "\"hits\":{\"total\":{\"value\":0,\"relation\":\"eq\"},\"max_score\":null,\"hits\":[]}}";
    Response scrollResponse = jsonResponse(200, scrollBody);
    when(restClient.performRequest(any(Request.class))).thenReturn(scrollResponse);
    SearchResponse scrolled =
        shimWith(restClient)
            .scroll(OP, new SearchScrollRequest("scroll-abc"), RequestOptions.DEFAULT);
    assertEquals(scrolled.getScrollId(), "scroll-abc");

    RestClient clearClient = mock(RestClient.class);
    Response clearResponse = jsonResponse(200, "{\"succeeded\":true,\"num_freed\":3}");
    when(clearClient.performRequest(any(Request.class))).thenReturn(clearResponse);
    ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
    clearScrollRequest.addScrollId("scroll-abc");
    var cleared = shimWith(clearClient).clearScroll(OP, clearScrollRequest, RequestOptions.DEFAULT);
    assertTrue(cleared.isSucceeded());
    assertEquals(cleared.getNumFreed(), 3);
  }

  @Test
  public void updateByQueryParsesBulkByScrollResponse() throws Exception {
    RestClient restClient = mock(RestClient.class);
    String body =
        "{\"took\":10,\"timed_out\":false,\"total\":2,\"updated\":2,\"created\":0,\"deleted\":0,"
            + "\"batches\":1,\"version_conflicts\":0,\"noops\":0,"
            + "\"retries\":{\"bulk\":0,\"search\":0},\"throttled_millis\":0,"
            + "\"requests_per_second\":-1.0,\"throttled_until_millis\":0,\"failures\":[]}";
    Response ok = jsonResponse(200, body);
    when(restClient.performRequest(any(Request.class))).thenReturn(ok);

    var response =
        shimWith(restClient)
            .updateByQuery(OP, new UpdateByQueryRequest("idx"), RequestOptions.DEFAULT);
    assertEquals(response.getUpdated(), 2L);
  }

  @Test
  public void cloneIndexParsesAcknowledgement() throws Exception {
    RestClient restClient = mock(RestClient.class);
    Response ok =
        jsonResponse(
            200, "{\"acknowledged\":true,\"shards_acknowledged\":true,\"index\":\"clone_target\"}");
    when(restClient.performRequest(any(Request.class))).thenReturn(ok);

    var response =
        shimWith(restClient)
            .cloneIndex(
                OP, new ResizeRequest("clone_target", "source_idx"), RequestOptions.DEFAULT);
    assertTrue(response.isAcknowledged());
  }

  @Test
  public void clusterSettingsRoundTrip() throws Exception {
    RestClient getClient = mock(RestClient.class);
    Response getResponse =
        jsonResponse(
            200,
            "{\"persistent\":{\"cluster.routing.allocation.enable\":\"all\"},\"transient\":{}}");
    when(getClient.performRequest(any(Request.class))).thenReturn(getResponse);
    var settings =
        shimWith(getClient)
            .getClusterSettings(new ClusterGetSettingsRequest(), RequestOptions.DEFAULT);
    assertEquals(settings.getPersistentSettings().get("cluster.routing.allocation.enable"), "all");

    RestClient putClient = mock(RestClient.class);
    Response putResponse =
        jsonResponse(200, "{\"acknowledged\":true,\"persistent\":{},\"transient\":{}}");
    when(putClient.performRequest(any(Request.class))).thenReturn(putResponse);
    ClusterUpdateSettingsRequest update = new ClusterUpdateSettingsRequest();
    update.persistentSettings(
        Settings.builder().put("cluster.routing.allocation.enable", "all").build());
    assertTrue(
        shimWith(putClient).putClusterSettings(update, RequestOptions.DEFAULT).isAcknowledged());
  }

  @Test
  public void clusterHealthParsesTimeoutStatus() throws Exception {
    RestClient restClient = mock(RestClient.class);
    String body =
        "{\"cluster_name\":\"c\",\"status\":\"red\",\"timed_out\":true,\"number_of_nodes\":1,"
            + "\"number_of_data_nodes\":1,\"active_primary_shards\":0,\"active_shards\":0,"
            + "\"relocating_shards\":0,\"initializing_shards\":0,\"unassigned_shards\":0,"
            + "\"delayed_unassigned_shards\":0,\"number_of_pending_tasks\":0,"
            + "\"number_of_in_flight_fetch\":0,\"task_max_waiting_in_queue_millis\":0,"
            + "\"active_shards_percent_as_number\":100.0}";
    // A health-timeout response arrives as HTTP 408 with a parseable health body.
    ResponseException timeout = responseException(408, body);
    when(restClient.performRequest(any(Request.class))).thenThrow(timeout);

    var health =
        shimWith(restClient).clusterHealth(new ClusterHealthRequest(), RequestOptions.DEFAULT);
    assertTrue(health.isTimedOut());
  }

  @Test
  public void bulkProcessorSyncPathExecutesBulkThroughBridge() throws Exception {
    RestClient restClient = mock(RestClient.class);
    String bulkBody =
        "{\"took\":5,\"errors\":false,\"items\":[{\"index\":{\"_index\":\"idx\",\"_id\":\"1\","
            + "\"_version\":1,\"result\":\"created\",\"status\":201,"
            + "\"_shards\":{\"total\":1,\"successful\":1,\"failed\":0},"
            + "\"_seq_no\":0,\"_primary_term\":1}}]}";
    Response bulkResponse = jsonResponse(200, bulkBody);
    when(restClient.performRequest(any(Request.class))).thenReturn(bulkResponse);

    OpenSearchSearchClientShim shim = shimWith(restClient);
    shim.generateBulkProcessor(
        WriteRequest.RefreshPolicy.NONE, mock(MetricUtils.class), 10, 10_000L, 1L, 0, 1);
    shim.addBulk(OP, "urn:li:test:1", new IndexRequest("idx").id("1").source(Map.of("f", "v")));
    shim.flushBulkProcessor();

    ArgumentCaptor<Request> sent = ArgumentCaptor.forClass(Request.class);
    org.mockito.Mockito.verify(restClient).performRequest(sent.capture());
    assertEquals(sent.getValue().getEndpoint(), "/_bulk");
  }

  @Test
  public void constructorBuildsRealClientWithAuthSslAndAwsSigning() throws Exception {
    ShimConfiguration config = mock(ShimConfiguration.class);
    when(config.getEngineType()).thenReturn(SearchEngineType.OPENSEARCH_2);
    when(config.getHost()).thenReturn("localhost");
    when(config.getPort()).thenReturn(9200);
    when(config.isUseSSL()).thenReturn(false);
    when(config.getPathPrefix()).thenReturn("/search");
    when(config.getThreadCount()).thenReturn(1);
    when(config.getConnectionRequestTimeout()).thenReturn(1000);
    when(config.getUsername()).thenReturn("user");
    when(config.getPassword()).thenReturn("pass");
    when(config.isUseAwsIamAuth()).thenReturn(true);
    when(config.getRegion()).thenReturn("us-west-2");
    when(config.getAwsCredentialsProvider()).thenReturn(mock(AwsCredentialsProvider.class));

    // Building the client performs no I/O; this exercises the full connection wiring (basic auth,
    // connection manager, SigV4 interceptor) that integration suites cover only for anonymous
    // localhost.
    try (OpenSearchSearchClientShim shim = new OpenSearchSearchClientShim(config)) {
      assertNotNull(shim.getNativeClient());
      assertEquals(shim.getEngineType(), SearchEngineType.OPENSEARCH_2);
    }

    ShimConfiguration missingRegion = mock(ShimConfiguration.class);
    when(missingRegion.getEngineType()).thenReturn(SearchEngineType.OPENSEARCH_2);
    when(missingRegion.getHost()).thenReturn("localhost");
    when(missingRegion.getPort()).thenReturn(9200);
    when(missingRegion.getThreadCount()).thenReturn(1);
    when(missingRegion.isUseAwsIamAuth()).thenReturn(true);
    assertThrows(
        IllegalArgumentException.class, () -> new OpenSearchSearchClientShim(missingRegion));

    // IAM auth on but no shared credential provider configured: reject rather than fall back to a
    // per-client DefaultCredentialsProvider (INC-5436 IRSA refresh leak).
    ShimConfiguration missingProvider = mock(ShimConfiguration.class);
    when(missingProvider.getEngineType()).thenReturn(SearchEngineType.OPENSEARCH_2);
    when(missingProvider.getHost()).thenReturn("localhost");
    when(missingProvider.getPort()).thenReturn(9200);
    when(missingProvider.getThreadCount()).thenReturn(1);
    when(missingProvider.getConnectionRequestTimeout()).thenReturn(1000);
    when(missingProvider.isUseAwsIamAuth()).thenReturn(true);
    when(missingProvider.getRegion()).thenReturn("us-west-2");
    assertThrows(
        IllegalStateException.class, () -> new OpenSearchSearchClientShim(missingProvider));
  }

  @Test
  public void constructorRejectsNonOpenSearchEngineType() {
    // Coercing an ES engine type to OPENSEARCH_2 would mask a mis-wired factory; reject instead.
    ShimConfiguration esConfig = mock(ShimConfiguration.class);
    when(esConfig.getEngineType()).thenReturn(SearchEngineType.ELASTICSEARCH_8);
    assertThrows(IllegalArgumentException.class, () -> new OpenSearchSearchClientShim(esConfig));

    ShimConfiguration nullEngine = mock(ShimConfiguration.class);
    when(nullEngine.getEngineType()).thenReturn(null);
    assertThrows(IllegalArgumentException.class, () -> new OpenSearchSearchClientShim(nullEngine));
  }
}
