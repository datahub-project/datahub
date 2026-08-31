package com.linkedin.metadata.search.embedding;

import static org.testng.Assert.*;

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.models.EmbeddingCreateParams;
import com.openai.models.EmbeddingModel;
import java.util.List;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Integration tests for OpenAIEmbeddingProvider that call the real OpenAI API.
 *
 * <p>These tests are skipped unless the OPENAI_API_KEY environment variable is set.
 *
 * <p>To run: OPENAI_API_KEY=sk-your-key ./gradlew :metadata-io:test --tests
 * "OpenAIEmbeddingProviderIntegrationTest"
 */
public class OpenAIEmbeddingProviderIntegrationTest {

  private static final String API_KEY_ENV = "OPENAI_API_KEY";
  private String apiKey;

  @BeforeClass
  public void setup() {
    apiKey = System.getenv(API_KEY_ENV);
    if (apiKey == null || apiKey.isBlank()) {
      throw new SkipException(
          "Skipping OpenAI integration tests: " + API_KEY_ENV + " environment variable not set");
    }
  }

  @Test
  public void testEmbedWithTextEmbedding3Small() {
    OpenAIEmbeddingProvider provider =
        new OpenAIEmbeddingProvider(
            apiKey, "https://api.openai.com/v1/embeddings", "text-embedding-3-small");

    float[] embedding = provider.embed("What is semantic search?", null);

    assertNotNull(embedding, "Embedding should not be null");
    assertEquals(embedding.length, 1536, "text-embedding-3-small should return 1536 dimensions");
    assertTrue(hasNonZeroValues(embedding), "Embedding should contain non-zero values");

    System.out.println(
        "OpenAI text-embedding-3-small: Successfully generated " + embedding.length + "d vector");
  }

  @Test
  public void testEmbedWithTextEmbedding3Large() {
    OpenAIEmbeddingProvider provider =
        new OpenAIEmbeddingProvider(
            apiKey, "https://api.openai.com/v1/embeddings", "text-embedding-3-large");

    float[] embedding = provider.embed("What is semantic search?", null);

    assertNotNull(embedding, "Embedding should not be null");
    assertEquals(embedding.length, 3072, "text-embedding-3-large should return 3072 dimensions");
    assertTrue(hasNonZeroValues(embedding), "Embedding should contain non-zero values");

    System.out.println(
        "OpenAI text-embedding-3-large: Successfully generated " + embedding.length + "d vector");
  }

  @Test
  public void testEmbedWithModelOverride() {
    // Create provider with small model but override to large
    OpenAIEmbeddingProvider provider =
        new OpenAIEmbeddingProvider(
            apiKey, "https://api.openai.com/v1/embeddings", "text-embedding-3-small");

    float[] embedding = provider.embed("Test query", "text-embedding-3-large");

    assertNotNull(embedding, "Embedding should not be null");
    assertEquals(
        embedding.length, 3072, "Model override to text-embedding-3-large should return 3072d");

    System.out.println(
        "OpenAI model override: Successfully generated " + embedding.length + "d vector");
  }

  @Test
  public void testEmbedSimilarTextsProduceSimilarVectors() {
    OpenAIEmbeddingProvider provider =
        new OpenAIEmbeddingProvider(
            apiKey, "https://api.openai.com/v1/embeddings", "text-embedding-3-small");

    float[] embedding1 = provider.embed("The cat sat on the mat", null);
    float[] embedding2 = provider.embed("A cat was sitting on a mat", null);
    float[] embedding3 = provider.embed("Quantum physics explains subatomic particles", null);

    double similarity12 = cosineSimilarity(embedding1, embedding2);
    double similarity13 = cosineSimilarity(embedding1, embedding3);

    System.out.println("Similarity (cat sentences): " + similarity12);
    System.out.println("Similarity (cat vs physics): " + similarity13);

    assertTrue(
        similarity12 > similarity13,
        "Similar sentences should have higher cosine similarity than unrelated sentences");
    assertTrue(similarity12 > 0.8, "Very similar sentences should have similarity > 0.8");
  }

  /**
   * Compares our custom implementation against the official OpenAI Java SDK to verify we get
   * identical embeddings for the same input.
   */
  @Test
  public void testMatchesOfficialOpenAISDK() {
    String testText = "DataHub is a modern data catalog for the modern data stack.";
    String model = "text-embedding-3-small";

    // Generate embedding with our implementation
    OpenAIEmbeddingProvider ourProvider =
        new OpenAIEmbeddingProvider(apiKey, "https://api.openai.com/v1/embeddings", model);
    float[] ourEmbedding = ourProvider.embed(testText, null);

    // Generate embedding with official SDK
    OpenAIClient sdkClient = OpenAIOkHttpClient.builder().apiKey(apiKey).build();

    EmbeddingCreateParams params =
        EmbeddingCreateParams.builder()
            .input(testText)
            .model(EmbeddingModel.TEXT_EMBEDDING_3_SMALL)
            .build();

    List<Double> sdkEmbeddingList = sdkClient.embeddings().create(params).data().get(0).embedding();

    // Convert SDK embedding to float[]
    float[] sdkEmbedding = new float[sdkEmbeddingList.size()];
    for (int i = 0; i < sdkEmbeddingList.size(); i++) {
      sdkEmbedding[i] = sdkEmbeddingList.get(i).floatValue();
    }

    // Verify dimensions match
    assertEquals(
        ourEmbedding.length,
        sdkEmbedding.length,
        "Our implementation and SDK should return same dimensions");

    // Note: OpenAI's embedding API is not perfectly deterministic - calling it twice
    // with the same input can produce slightly different values. We verify using
    // cosine similarity which should be extremely high (>0.999) for the same input.
    double similarity = cosineSimilarity(ourEmbedding, sdkEmbedding);

    System.out.println(
        "Dimensions - Ours: " + ourEmbedding.length + ", SDK: " + sdkEmbedding.length);
    System.out.println("Cosine similarity between our impl and SDK: " + similarity);
    System.out.println("First 5 values - Ours: " + formatFirstN(ourEmbedding, 5));
    System.out.println("First 5 values - SDK:  " + formatFirstN(sdkEmbedding, 5));

    // The embeddings should be nearly identical (API non-determinism causes minor differences)
    assertTrue(
        similarity > 0.999,
        "Our implementation should produce nearly identical embeddings to SDK. "
            + "Similarity: "
            + similarity);

    System.out.println("Embeddings match with similarity: " + similarity);
  }

  /**
   * Verifies that for the same input, our implementation produces embeddings with the same
   * dimensions as the official SDK across different models.
   */
  @Test
  public void testDimensionsMatchSDKForAllModels() {
    String testText = "Test input for dimension verification";

    // Test text-embedding-3-small (1536 dimensions)
    verifyDimensionsMatchSDK(
        testText, "text-embedding-3-small", EmbeddingModel.TEXT_EMBEDDING_3_SMALL, 1536);

    // Test text-embedding-3-large (3072 dimensions)
    verifyDimensionsMatchSDK(
        testText, "text-embedding-3-large", EmbeddingModel.TEXT_EMBEDDING_3_LARGE, 3072);
  }

  private void verifyDimensionsMatchSDK(
      String text, String modelName, EmbeddingModel sdkModel, int expectedDimensions) {
    // Our implementation
    OpenAIEmbeddingProvider ourProvider =
        new OpenAIEmbeddingProvider(apiKey, "https://api.openai.com/v1/embeddings", modelName);
    float[] ourEmbedding = ourProvider.embed(text, null);

    // SDK
    OpenAIClient sdkClient = OpenAIOkHttpClient.builder().apiKey(apiKey).build();
    EmbeddingCreateParams params =
        EmbeddingCreateParams.builder().input(text).model(sdkModel).build();
    int sdkDimensions = sdkClient.embeddings().create(params).data().get(0).embedding().size();

    assertEquals(ourEmbedding.length, expectedDimensions, "Our impl dimensions for " + modelName);
    assertEquals(sdkDimensions, expectedDimensions, "SDK dimensions for " + modelName);
    assertEquals(
        ourEmbedding.length,
        sdkDimensions,
        "Dimensions should match between our impl and SDK for " + modelName);

    System.out.println(
        modelName + ": Both implementations returned " + expectedDimensions + " dimensions");
  }

  private boolean hasNonZeroValues(float[] embedding) {
    for (float v : embedding) {
      if (v != 0.0f) {
        return true;
      }
    }
    return false;
  }

  private double cosineSimilarity(float[] a, float[] b) {
    if (a.length != b.length) {
      throw new IllegalArgumentException("Vectors must have same length");
    }
    double dotProduct = 0.0;
    double normA = 0.0;
    double normB = 0.0;
    for (int i = 0; i < a.length; i++) {
      dotProduct += a[i] * b[i];
      normA += a[i] * a[i];
      normB += b[i] * b[i];
    }
    return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
  }

  private String formatFirstN(float[] arr, int n) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < Math.min(n, arr.length); i++) {
      if (i > 0) sb.append(", ");
      sb.append(String.format("%.6f", arr[i]));
    }
    sb.append("]");
    return sb.toString();
  }
}
