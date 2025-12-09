/**
 * SAAS-SPECIFIC: This integration test is part of the semantic search feature exclusive to DataHub
 * SaaS. It should NOT be merged back to the open-source DataHub repository. Dependencies: Requires
 * DataHub Integrations Service and AWS Bedrock Cohere.
 */
package com.linkedin.metadata.search.embedding;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.integration.IntegrationsService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Integration test for {@link IntegrationsServiceEmbeddingProvider}.
 *
 * <p>This test verifies that the embedding provider can successfully call the integrations service
 * and receive properly formatted embeddings. It connects to the running integrations service to
 * ensure end-to-end functionality.
 *
 * <p>Prerequisites: The integrations service must be running on localhost:9003 (use ./gradlew
 * quickstartDebugComposeUp to start it)
 */
@Test(enabled = false) // Disabled: Requires integrations service to be running
public class IntegrationsServiceEmbeddingProviderIntegrationTest {

  private static final String INTEGRATIONS_SERVICE_HOST = "localhost";
  private static final int INTEGRATIONS_SERVICE_PORT = 9003;

  private IntegrationsService integrationsService;
  private IntegrationsServiceEmbeddingProvider embeddingProvider;
  private OperationContext systemOperationContext;

  @BeforeClass
  public void setup() throws Exception {
    // Create system operation context
    systemOperationContext = TestOperationContexts.systemContextNoSearchAuthorization();

    // Create IntegrationsService instance pointing to the running service
    integrationsService =
        new IntegrationsService(
            INTEGRATIONS_SERVICE_HOST,
            INTEGRATIONS_SERVICE_PORT,
            false, // no SSL for test
            systemOperationContext);

    // Create the embedding provider
    embeddingProvider = new IntegrationsServiceEmbeddingProvider(integrationsService);
  }

  public void testEmbedQuery_SimpleText_ReturnsValidEmbedding() {
    // Given
    String text = "test query for embeddings";
    String model = "cohere.embed-english-v3";

    // When
    float[] embedding = embeddingProvider.embed(text, model);

    // Then
    assertNotNull(embedding, "Embedding should not be null");
    assertEquals(embedding.length, 1024, "Embedding should have 1024 dimensions");

    // Verify L2 normalization (should be close to 1.0)
    double norm = 0.0;
    for (float value : embedding) {
      norm += value * value;
    }
    norm = Math.sqrt(norm);
    assertTrue(
        Math.abs(norm - 1.0) < 0.01, "Embedding should be L2-normalized, but norm was: " + norm);
  }

  public void testEmbedQuery_WithModel_ReturnsValidEmbedding() {
    // Given
    String text = "test query with specific model";
    String model = "cohere.embed-english-v3";

    // When
    float[] embedding = embeddingProvider.embed(text, model);

    // Then
    assertNotNull(embedding, "Embedding should not be null");
    assertEquals(embedding.length, 1024, "Embedding should have 1024 dimensions");
  }

  public void testEmbedQuery_LongText_HandlesGracefully() {
    // Given - text that stays within Cohere model limit (max 2048 chars)
    String longText = "a".repeat(10000);
    String model = "cohere.embed-english-v3";

    // When
    float[] embedding = embeddingProvider.embed(longText, model);

    // Then
    assertNotNull(embedding, "Embedding should not be null even for long text");
    assertEquals(embedding.length, 1024, "Embedding should have 1024 dimensions");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testEmbedQuery_EmptyText_ThrowsException() {
    // Given
    String emptyText = "";
    String model = "cohere.embed-english-v3";

    // When/Then - should throw
    embeddingProvider.embed(emptyText, model);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testEmbedQuery_NullText_ThrowsException() {
    // Given
    String model = "cohere.embed-english-v3";

    // When/Then - should throw
    embeddingProvider.embed(null, model);
  }

  public void testEmbedQuery_ConsistentResults() {
    // Given
    String text = "consistent test query";
    String model = "cohere.embed-english-v3";

    // When - call twice with same input
    float[] embedding1 = embeddingProvider.embed(text, model);
    float[] embedding2 = embeddingProvider.embed(text, model);

    // Then - should return embeddings with same dimensions
    assertNotNull(embedding1);
    assertNotNull(embedding2);
    assertEquals(embedding1.length, embedding2.length);

    // Embeddings should be similar but may have small variations
    // Calculate cosine similarity to ensure consistency
    double dotProduct = 0.0;
    double norm1 = 0.0;
    double norm2 = 0.0;
    for (int i = 0; i < embedding1.length; i++) {
      dotProduct += embedding1[i] * embedding2[i];
      norm1 += embedding1[i] * embedding1[i];
      norm2 += embedding2[i] * embedding2[i];
    }
    double cosineSimilarity = dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2));

    // Should be very similar (> 0.99) for same input
    assertTrue(
        cosineSimilarity > 0.99,
        "Embeddings should be consistent for same input, cosine similarity was: "
            + cosineSimilarity);
  }

  public void testEmbedQuery_PerformanceCheck() {
    // Given
    String text = "performance test query";
    String model = "cohere.embed-english-v3";
    int iterations = 10;

    // When
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < iterations; i++) {
      float[] embedding = embeddingProvider.embed(text, model);
      assertNotNull(embedding);
    }
    long endTime = System.currentTimeMillis();

    // Then
    long totalTime = endTime - startTime;
    double avgTime = (double) totalTime / iterations;

    // Log performance metrics
    System.out.println(
        String.format(
            "Performance: %d embeddings in %dms (avg: %.2fms per embedding)",
            iterations, totalTime, avgTime));

    // Assert reasonable performance (adjust threshold as needed)
    assertTrue(
        avgTime < 1000,
        "Average embedding time should be less than 1 second, but was: " + avgTime + "ms");
  }
}
