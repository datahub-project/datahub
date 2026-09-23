package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.testng.Assert.*;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.utils.elasticsearch.shim.EmbeddingBatch;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchResponse;
import com.linkedin.metadata.utils.elasticsearch.shim.SemanticIndexSpec;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Testcontainers integration test for ES 8.18 semantic search.
 *
 * <p>Brings up a real ES 8.18 container and verifies the createIndex → indexEmbeddings → searchKnn
 * round-trip, dimension-mismatch rejection, and filtered kNN with the bool filters semantic search
 * builds.
 *
 * <p>Requires Docker. When Docker is unavailable the {@code @BeforeClass} method throws {@link
 * SkipException} so the tests are recorded as skipped rather than failed.
 */
public class Es8SemanticSearchIT {

  private static final String IMAGE = "docker.elastic.co/elasticsearch/elasticsearch:8.18.0";

  private ElasticsearchContainer container;
  private Es8SearchClientShim shim;
  private final ObjectMapper objectMapper = new ObjectMapper();

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    try {
      container =
          new ElasticsearchContainer(
                  DockerImageName.parse(IMAGE)
                      .asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch"))
              .withEnv("xpack.security.enabled", "false")
              .withEnv("discovery.type", "single-node")
              .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m");
      container.start();
    } catch (org.testcontainers.containers.ContainerLaunchException | IllegalStateException e) {
      // Rethrow as SkipException only when Docker is genuinely unavailable; programmer errors
      // (misconfigured images, NullPointerException from refactors, etc.) propagate as failures
      // so they don't get silently swallowed and reported as skips.
      String msg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
      if (msg.contains("docker") || msg.contains("daemon") || msg.contains("container")) {
        throw new SkipException("Docker not available or ES container failed to start: " + msg, e);
      }
      throw e;
    }

    RestClient restClient =
        RestClient.builder(HttpHost.create(container.getHttpHostAddress())).build();
    ElasticsearchClient client =
        new ElasticsearchClient(new RestClientTransport(restClient, new JacksonJsonpMapper()));
    shim = Es8SearchClientShim.forTest(client);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    if (container != null && container.isRunning()) {
      container.stop();
    }
  }

  @Test(groups = "es8-semantic")
  public void testCreateIndexAndSearchRoundTrip() throws Exception {
    SemanticIndexSpec spec =
        SemanticIndexSpec.builder()
            .indexName("doc_v2_semantic")
            .modelKey("gemini_embedding_001")
            .vectorDimension(4)
            .build();

    shim.createSemanticIndex(spec);

    // Index two documents with clearly separated vectors
    EmbeddingBatch.Chunk c1 =
        new EmbeddingBatch.Chunk(new float[] {0.9f, 0.1f, 0.0f, 0.0f}, "alpha", 0, 0, 5, 1);
    shim.indexEmbeddings(
        OP_CONTEXT,
        new EmbeddingBatch("doc_v2_semantic", "urn:doc:1", "gemini_embedding_001", List.of(c1)));

    EmbeddingBatch.Chunk c2 =
        new EmbeddingBatch.Chunk(new float[] {0.0f, 0.0f, 0.1f, 0.9f}, "beta", 0, 0, 4, 1);
    shim.indexEmbeddings(
        OP_CONTEXT,
        new EmbeddingBatch("doc_v2_semantic", "urn:doc:2", "gemini_embedding_001", List.of(c2)));

    // Explicitly refresh the index so documents are immediately searchable without a sleep.
    shim.getNativeClient().indices().refresh(r -> r.index("doc_v2_semantic"));

    KnnSearchResponse out =
        shim.searchKnn(
            OP_CONTEXT,
            KnnSearchRequest.builder()
                .indexName("doc_v2_semantic")
                .vectorField("embeddings.gemini_embedding_001.chunks.vector")
                .queryVector(new float[] {0.95f, 0.0f, 0.0f, 0.0f})
                .k(2)
                .build());

    assertEquals(out.hits().size(), 2, "Expected 2 hits");
    // urn:doc:1 is closest to the query vector (alpha direction)
    assertEquals(out.hits().get(0).id(), "urn:doc:1", "Top hit should be urn:doc:1");
  }

  @Test(groups = "es8-semantic")
  public void testDimensionMismatchRejected() throws Exception {
    SemanticIndexSpec spec =
        SemanticIndexSpec.builder()
            .indexName("dimcheck_semantic")
            .modelKey("m")
            .vectorDimension(4)
            .build();
    shim.createSemanticIndex(spec);

    EmbeddingBatch wrongDim =
        new EmbeddingBatch(
            "dimcheck_semantic",
            "urn:1",
            "m",
            List.of(new EmbeddingBatch.Chunk(new float[] {0.1f, 0.2f}, "x", 0, 0, 1, 1)));

    try {
      shim.indexEmbeddings(OP_CONTEXT, wrongDim);
      fail("Expected an exception for dimension mismatch (vector has 2 dims, index expects 4)");
    } catch (ElasticsearchException expected) {
      // ES rejects the document because the vector dimension does not match the mapping.
      // The error message should reference dims or vectors — asserting this ensures we caught the
      // right failure and not some other unrelated exception.
      String msg = expected.getMessage().toLowerCase();
      assertTrue(
          msg.contains("dim") || msg.contains("vector"),
          "Exception should mention dimension or vector mismatch; got: " + expected.getMessage());
    }
  }

  /**
   * DataHub's semantic kNN filters are nested OpenSearch bool queries whose serialization emits
   * {@code adjust_pure_negative} at every level. Before the fix, {@code searchKnn}'s strict {@code
   * SearchRequest#withJson} parse rejected that field, so any filtered semantic search failed on
   * Elasticsearch 8. This drives {@code searchKnn} end-to-end against ES 8.18 with such filters and
   * checks they still select the right documents, including a pure {@code must_not}.
   */
  @Test(groups = "es8-semantic")
  public void testSearchKnnAppliesLegacyBoolFilters() throws Exception {
    SemanticIndexSpec spec =
        SemanticIndexSpec.builder()
            .indexName("doc_filter_semantic")
            .modelKey("gemini_embedding_001")
            .vectorDimension(4)
            .build();
    shim.createSemanticIndex(spec);

    shim.indexEmbeddings(
        OP_CONTEXT,
        new EmbeddingBatch(
            "doc_filter_semantic",
            "urn:doc:1",
            "gemini_embedding_001",
            List.of(
                new EmbeddingBatch.Chunk(
                    new float[] {0.9f, 0.1f, 0.0f, 0.0f}, "alpha", 0, 0, 5, 1))));
    shim.indexEmbeddings(
        OP_CONTEXT,
        new EmbeddingBatch(
            "doc_filter_semantic",
            "urn:doc:2",
            "gemini_embedding_001",
            List.of(
                new EmbeddingBatch.Chunk(
                    new float[] {0.0f, 0.0f, 0.1f, 0.9f}, "beta", 0, 0, 4, 1))));
    shim.getNativeClient().indices().refresh(r -> r.index("doc_filter_semantic"));

    // Nested OpenSearch bool filter, the shape semantic search builds: its serialization carries
    // adjust_pure_negative, which the ES 8 typed parser rejects unless searchKnn normalizes it.
    // It selects the document farther from the query vector.
    assertEquals(
        filteredKnnIds(
            QueryBuilders.boolQuery()
                .must(
                    QueryBuilders.boolQuery().should(QueryBuilders.termQuery("urn", "urn:doc:2")))),
        List.of("urn:doc:2"));

    // A pure must_not keeps its "everything except" meaning once adjust_pure_negative is dropped
    assertEquals(
        filteredKnnIds(
            QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("urn", "urn:doc:1"))),
        List.of("urn:doc:2"));
  }

  /**
   * A selective filter must pre-filter: with k=2 over ten documents, the two nearest documents that
   * match are returned even though neither is among the two nearest overall. Applying the filter
   * after the kNN step would return nothing here.
   */
  @Test(groups = "es8-semantic")
  public void testSearchKnnPreFiltersBeforeTopK() throws Exception {
    shim.createSemanticIndex(
        SemanticIndexSpec.builder()
            .indexName("doc_prefilter_semantic")
            .modelKey("gemini_embedding_001")
            .vectorDimension(4)
            .build());
    for (int i = 0; i < 10; i++) {
      shim.indexEmbeddings(
          OP_CONTEXT,
          new EmbeddingBatch(
              "doc_prefilter_semantic",
              "urn:doc:" + i,
              "gemini_embedding_001",
              List.of(
                  new EmbeddingBatch.Chunk(
                      new float[] {1.0f, 0.3f * i, 0.0f, 0.0f}, "doc " + i, 0, 0, 5, 1))));
    }
    shim.getNativeClient().indices().refresh(r -> r.index("doc_prefilter_semantic"));

    @SuppressWarnings("unchecked")
    Map<String, Object> filter =
        objectMapper.readValue(
            QueryBuilders.termsQuery("urn", "urn:doc:7", "urn:doc:8", "urn:doc:9").toString(),
            Map.class);
    KnnSearchResponse out =
        shim.searchKnn(
            OP_CONTEXT,
            KnnSearchRequest.builder()
                .indexName("doc_prefilter_semantic")
                .vectorField("embeddings.gemini_embedding_001.chunks.vector")
                .queryVector(new float[] {1.0f, 0.0f, 0.0f, 0.0f})
                .k(2)
                .filter(filter)
                .build());

    assertEquals(
        out.hits().stream().map(KnnSearchResponse.Hit::id).toList(),
        List.of("urn:doc:7", "urn:doc:8"));
  }

  private List<String> filteredKnnIds(QueryBuilder filterQuery) throws Exception {
    @SuppressWarnings("unchecked")
    Map<String, Object> filter = objectMapper.readValue(filterQuery.toString(), Map.class);
    KnnSearchResponse out =
        shim.searchKnn(
            OP_CONTEXT,
            KnnSearchRequest.builder()
                .indexName("doc_filter_semantic")
                .vectorField("embeddings.gemini_embedding_001.chunks.vector")
                .queryVector(new float[] {0.95f, 0.0f, 0.0f, 0.0f})
                .k(2)
                .filter(filter)
                .build());
    return out.hits().stream().map(KnnSearchResponse.Hit::id).toList();
  }
}
