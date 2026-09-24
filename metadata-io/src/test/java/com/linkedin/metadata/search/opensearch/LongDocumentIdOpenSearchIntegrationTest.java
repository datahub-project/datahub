package com.linkedin.metadata.search.opensearch;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.SearchEngineType;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.CreateIndexRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/**
 * Pins the OpenSearch document-id length behavior DataHub depends on. Schema-field URNs are exempt
 * from URN length validation and (with doc-id hashing disabled, the default) are used URL-encoded
 * as document ids, so ids over 512 bytes occur in real deployments. OpenSearch rejects such ids at
 * index time; this test documents that boundary on both 2.x and 3.x so a behavior change in either
 * direction is caught when the suite runs against a new engine version.
 */
@Import({
  OpenSearchSuite.class,
  SearchCommonTestConfiguration.class,
  SearchTestContainerConfiguration.class
})
public class LongDocumentIdOpenSearchIntegrationTest extends AbstractTestNGSpringContextTests {

  private static final String TEST_INDEX = "test-long-doc-id-index";
  private static final OperationFingerprint OP = OperationFingerprint.EMPTY;

  @Autowired private SearchClientShim<?> searchClientShim;

  private void ensureIndex() throws Exception {
    if (!searchClientShim.indexExists(
        OP,
        new org.opensearch.client.indices.GetIndexRequest(TEST_INDEX),
        RequestOptions.DEFAULT)) {
      searchClientShim.createIndex(OP, new CreateIndexRequest(TEST_INDEX), RequestOptions.DEFAULT);
    }
  }

  @Test
  public void documentIdsUpTo512BytesIndexOnAllVersions() throws Exception {
    ensureIndex();
    String id512 = "u".repeat(512);
    assertEquals(id512.getBytes(StandardCharsets.UTF_8).length, 512);

    IndexRequest indexRequest =
        new IndexRequest(TEST_INDEX).id(id512).source(Map.of("field", "value"));
    searchClientShim.indexDocument(OP, indexRequest, RequestOptions.DEFAULT);

    assertTrue(
        searchClientShim
            .getDocument(OP, new GetRequest(TEST_INDEX, id512), RequestOptions.DEFAULT)
            .isExists());
  }

  @Test
  public void documentIdsOver512BytesAreRejected() throws Exception {
    ensureIndex();
    // Simulates a long URL-encoded schema-field URN used as a document id.
    String longId = "urn%3Ali%3AschemaField%3A" + "f".repeat(600);
    assertTrue(longId.getBytes(StandardCharsets.UTF_8).length > 512);

    IndexRequest indexRequest =
        new IndexRequest(TEST_INDEX).id(longId).source(Map.of("field", "value"));
    boolean rejected;
    try {
      searchClientShim.indexDocument(OP, indexRequest, RequestOptions.DEFAULT);
      rejected = false;
    } catch (OpenSearchStatusException | java.io.IOException e) {
      rejected = true;
    }

    // Both 2.x and 3.x reject oversized ids on the single-document path (the bulk path is pinned
    // separately below). If a future engine version changes this, the assertion below fails and
    // the doc-id hashing guidance (see ElasticSearchServiceFactory#warnOpenSearch3DocIdRisk) must
    // be revisited.
    assertTrue(
        rejected,
        "Expected " + searchClientShim.getEngineType() + " to reject a document id over 512 bytes");
    assertFalse(
        searchClientShim
            .getDocument(OP, new GetRequest(TEST_INDEX, longId), RequestOptions.DEFAULT)
            .isExists());
  }

  /**
   * The bulk path is the one {@code warnOpenSearch3DocIdRisk} is about: long schema-field URNs
   * reach the engine through {@code _bulk}, not the single-document API. Empirically both lines
   * reject an oversized id there — 2.19 fails the whole request with a 400
   * action_request_validation_exception, 3.x reports it per item — so pin "rejected, one way or the
   * other" and fail if a future engine version starts accepting oversized bulk ids.
   */
  @Test
  public void bulkPathRejectsIdsOver512Bytes() throws Exception {
    ensureIndex();
    String longId = "urn%3Ali%3AschemaField%3A" + "b".repeat(600);

    org.opensearch.action.bulk.BulkRequest bulkRequest =
        new org.opensearch.action.bulk.BulkRequest();
    bulkRequest.add(new IndexRequest(TEST_INDEX).id(longId).source(Map.of("field", "value")));

    org.opensearch.client.Request lowLevel =
        org.opensearch.client.OpenSearchShimBridge.bulk(bulkRequest);
    boolean rejected;
    try {
      org.opensearch.client.Response response =
          ((org.opensearch.client.RestClient) searchClientShim.getNativeClient())
              .performRequest(lowLevel);
      com.fasterxml.jackson.databind.JsonNode item =
          new com.fasterxml.jackson.databind.ObjectMapper()
              .readTree(org.apache.http.util.EntityUtils.toString(response.getEntity()))
              .path("items")
              .get(0)
              .path("index");
      rejected = item.path("status").asInt() >= 400;
    } catch (org.opensearch.client.ResponseException e) {
      // 2.x path: request-level action_request_validation_exception (HTTP 400).
      rejected = true;
    }

    assertTrue(
        rejected,
        "Expected "
            + searchClientShim.getEngineType()
            + " to reject a >512-byte document id on the bulk path");
    assertFalse(
        searchClientShim
            .getDocument(OP, new GetRequest(TEST_INDEX, longId), RequestOptions.DEFAULT)
            .isExists());
  }

  @Test
  public void engineTypeIsOpenSearchFamily() {
    SearchEngineType engineType = searchClientShim.getEngineType();
    assertTrue(engineType.isOpenSearch());
    assertTrue(engineType.requiresOpenSearchClient());
  }
}
