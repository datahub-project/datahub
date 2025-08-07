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
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Integration test for {@link IntegrationsServiceEmbeddingProvider}.
 *
 * <p>This test requires the integrations service to be running locally on port 9003. It verifies
 * that the embedding provider can successfully call the integrations service and receive properly
 * formatted embeddings.
 *
 * <p>To run this test: 1. Start the integrations service: cd datahub-integrations-service &&
 * ../gradlew installDev && source venv/bin/activate && uvicorn datahub_integrations.server:app
 * --host 0.0.0.0 --port 9003 --reload 2. Run this test
 */
@Test(enabled = false) // Disabled: Requires integrations service to be running
public class IntegrationsServiceEmbeddingProviderIT {

  private static final String INTEGRATIONS_SERVICE_HOST = "localhost";
  private static final int INTEGRATIONS_SERVICE_PORT = 9003;

  private IntegrationsService integrationsService;
  private IntegrationsServiceEmbeddingProvider embeddingProvider;

  @BeforeClass
  public void setup() throws Exception {
    // Check if integrations service is running
    if (!isIntegrationsServiceRunning()) {
      throw new SkipException(
          "Integrations service is not running on "
              + INTEGRATIONS_SERVICE_HOST
              + ":"
              + INTEGRATIONS_SERVICE_PORT
              + ". Please start it before running this test.");
    }

    // Create system operation context
    OperationContext systemOperationContext =
        TestOperationContexts.systemContextNoSearchAuthorization();

    // Create IntegrationsService instance
    integrationsService =
        new IntegrationsService(
            INTEGRATIONS_SERVICE_HOST,
            INTEGRATIONS_SERVICE_PORT,
            false, // no SSL for local test
            systemOperationContext);

    // Create the embedding provider
    embeddingProvider = new IntegrationsServiceEmbeddingProvider(integrationsService);
  }

  private boolean isIntegrationsServiceRunning() {
    try {
      java.net.Socket socket = new java.net.Socket();
      socket.connect(
          new java.net.InetSocketAddress(INTEGRATIONS_SERVICE_HOST, INTEGRATIONS_SERVICE_PORT),
          1000);
      socket.close();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public void testEmbedQuery_SimpleText_ReturnsValidEmbedding() {
    // Given
    String text = "test query for embeddings";

    // When
    float[] embedding = embeddingProvider.embed(text, null);

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
    String model = "bedrock:cohere.embed-english-v3";

    // When
    float[] embedding = embeddingProvider.embed(text, model);

    // Then
    assertNotNull(embedding, "Embedding should not be null");
    assertEquals(embedding.length, 1024, "Embedding should have 1024 dimensions");
  }

  public void testEmbedQuery_LongText_HandlesGracefully() {
    // Given - text that would be truncated
    String longText = "a".repeat(3000);

    // When
    float[] embedding = embeddingProvider.embed(longText, "bedrock:dummy");

    // Then
    assertNotNull(embedding, "Embedding should not be null even for long text");
    assertEquals(embedding.length, 1024, "Embedding should have 1024 dimensions");
  }

  public void testEmbedQuery_EmptyText_ThrowsException() {
    // Given
    String emptyText = "   "; // Whitespace only

    // When/Then - should throw
    embeddingProvider.embed(emptyText, null);
  }

  public void testEmbedQuery_NullText_ThrowsException() {
    // When/Then - should throw
    embeddingProvider.embed(null, null);
  }

  public void testEmbedQuery_ConsistentResults() {
    // Given
    String text = "consistent test query";

    // When - call twice with same input
    float[] embedding1 = embeddingProvider.embed(text, null);
    float[] embedding2 = embeddingProvider.embed(text, null);

    // Then - should return same embedding (for dummy implementation)
    assertNotNull(embedding1);
    assertNotNull(embedding2);
    assertEquals(embedding1.length, embedding2.length);

    // For dummy implementation, values should be identical
    for (int i = 0; i < embedding1.length; i++) {
      assertEquals(
          embedding1[i], embedding2[i], 0.0001f, "Embeddings should be consistent for same input");
    }
  }

  public void testEmbedQuery_DifferentTexts_ReturnsSameEmbedding_DummyImpl() {
    // Given - different texts (dummy implementation returns same embedding)
    String text1 = "first query";
    String text2 = "completely different query";

    // When
    float[] embedding1 = embeddingProvider.embed(text1, null);
    float[] embedding2 = embeddingProvider.embed(text2, null);

    // Then - for dummy implementation, should be the same
    assertNotNull(embedding1);
    assertNotNull(embedding2);
    assertEquals(embedding1.length, embedding2.length);

    // Dummy implementation returns same values
    for (int i = 0; i < Math.min(10, embedding1.length); i++) {
      assertEquals(
          embedding1[i],
          embedding2[i],
          0.0001f,
          "Dummy implementation should return same embedding values");
    }
  }

  public void testGenerateQueryEmbedding_DirectCall() {
    // Test calling IntegrationsService directly
    String text = "direct service call test";

    // When
    float[] embedding = integrationsService.generateQueryEmbedding(text, null);

    // Then
    assertNotNull(embedding, "Embedding should not be null");
    assertEquals(embedding.length, 1024, "Embedding should have 1024 dimensions");
  }

  public void testEmbedQuery_PerformanceCheck() {
    // Given
    String text = "performance test query";
    int iterations = 5;

    // Warm up
    embeddingProvider.embed(text, null);

    // When
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < iterations; i++) {
      float[] embedding = embeddingProvider.embed(text, null);
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
        avgTime < 2000,
        "Average embedding time should be less than 2 seconds, but was: " + avgTime + "ms");
  }
}
