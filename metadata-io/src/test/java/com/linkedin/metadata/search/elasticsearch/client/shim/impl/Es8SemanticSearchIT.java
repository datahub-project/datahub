package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.testng.Assert.*;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.linkedin.metadata.utils.elasticsearch.shim.EmbeddingBatch;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchResponse;
import com.linkedin.metadata.utils.elasticsearch.shim.SemanticIndexSpec;
import java.util.List;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
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
 * round-trip, plus dimension-mismatch rejection.
 *
 * <p>Requires Docker. When Docker is unavailable the {@code @BeforeClass} method throws {@link
 * SkipException} so the tests are recorded as skipped rather than failed.
 */
public class Es8SemanticSearchIT {

  private static final String IMAGE = "docker.elastic.co/elasticsearch/elasticsearch:8.18.0";

  private ElasticsearchContainer container;
  private Es8SearchClientShim shim;

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
        new EmbeddingBatch("doc_v2_semantic", "urn:doc:1", "gemini_embedding_001", List.of(c1)));

    EmbeddingBatch.Chunk c2 =
        new EmbeddingBatch.Chunk(new float[] {0.0f, 0.0f, 0.1f, 0.9f}, "beta", 0, 0, 4, 1);
    shim.indexEmbeddings(
        new EmbeddingBatch("doc_v2_semantic", "urn:doc:2", "gemini_embedding_001", List.of(c2)));

    // Explicitly refresh the index so documents are immediately searchable without a sleep.
    shim.getNativeClient().indices().refresh(r -> r.index("doc_v2_semantic"));

    KnnSearchResponse out =
        shim.searchKnn(
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
      shim.indexEmbeddings(wrongDim);
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
}
