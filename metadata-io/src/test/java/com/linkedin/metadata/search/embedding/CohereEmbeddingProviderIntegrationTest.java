package com.linkedin.metadata.search.embedding;

import static org.testng.Assert.*;

import com.cohere.api.Cohere;
import com.cohere.api.requests.EmbedRequest;
import com.cohere.api.types.EmbedFloatsResponse;
import com.cohere.api.types.EmbedInputType;
import com.cohere.api.types.EmbedResponse;
import java.util.List;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Integration tests for CohereEmbeddingProvider that call the real Cohere API.
 *
 * <p>These tests are skipped unless the COHERE_API_KEY environment variable is set.
 *
 * <p>To run: COHERE_API_KEY=your-key ./gradlew :metadata-io:test --tests
 * "CohereEmbeddingProviderIntegrationTest"
 */
public class CohereEmbeddingProviderIntegrationTest {

  private static final String API_KEY_ENV = "COHERE_API_KEY";
  private String apiKey;

  @BeforeClass
  public void setup() {
    apiKey = System.getenv(API_KEY_ENV);
    if (apiKey == null || apiKey.isBlank()) {
      throw new SkipException(
          "Skipping Cohere integration tests: " + API_KEY_ENV + " environment variable not set");
    }
  }

  @Test
  public void testEmbedWithEmbedEnglishV3() {
    CohereEmbeddingProvider provider =
        new CohereEmbeddingProvider(apiKey, "https://api.cohere.ai/v1/embed", "embed-english-v3.0");

    float[] embedding = provider.embed("What is semantic search?", null);

    assertNotNull(embedding, "Embedding should not be null");
    assertEquals(embedding.length, 1024, "embed-english-v3.0 should return 1024 dimensions");
    assertTrue(hasNonZeroValues(embedding), "Embedding should contain non-zero values");

    System.out.println(
        "Cohere embed-english-v3.0: Successfully generated " + embedding.length + "d vector");
  }

  @Test
  public void testEmbedWithEmbedMultilingualV3() {
    CohereEmbeddingProvider provider =
        new CohereEmbeddingProvider(
            apiKey, "https://api.cohere.ai/v1/embed", "embed-multilingual-v3.0");

    float[] embedding = provider.embed("What is semantic search?", null);

    assertNotNull(embedding, "Embedding should not be null");
    assertEquals(embedding.length, 1024, "embed-multilingual-v3.0 should return 1024 dimensions");
    assertTrue(hasNonZeroValues(embedding), "Embedding should contain non-zero values");

    System.out.println(
        "Cohere embed-multilingual-v3.0: Successfully generated " + embedding.length + "d vector");
  }

  @Test
  public void testEmbedWithEmbedEnglishLightV3() {
    CohereEmbeddingProvider provider =
        new CohereEmbeddingProvider(
            apiKey, "https://api.cohere.ai/v1/embed", "embed-english-light-v3.0");

    float[] embedding = provider.embed("What is semantic search?", null);

    assertNotNull(embedding, "Embedding should not be null");
    assertEquals(embedding.length, 384, "embed-english-light-v3.0 should return 384 dimensions");
    assertTrue(hasNonZeroValues(embedding), "Embedding should contain non-zero values");

    System.out.println(
        "Cohere embed-english-light-v3.0: Successfully generated " + embedding.length + "d vector");
  }

  @Test
  public void testEmbedWithModelOverride() {
    // Create provider with english model but override to multilingual
    CohereEmbeddingProvider provider =
        new CohereEmbeddingProvider(apiKey, "https://api.cohere.ai/v1/embed", "embed-english-v3.0");

    float[] embedding = provider.embed("Test query", "embed-multilingual-v3.0");

    assertNotNull(embedding, "Embedding should not be null");
    assertEquals(embedding.length, 1024, "Model override should still return 1024d");

    System.out.println(
        "Cohere model override: Successfully generated " + embedding.length + "d vector");
  }

  @Test
  public void testEmbedSimilarTextsProduceSimilarVectors() {
    CohereEmbeddingProvider provider =
        new CohereEmbeddingProvider(apiKey, "https://api.cohere.ai/v1/embed", "embed-english-v3.0");

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

  @Test
  public void testEmbedMultilingualWithNonEnglishText() {
    CohereEmbeddingProvider provider =
        new CohereEmbeddingProvider(
            apiKey, "https://api.cohere.ai/v1/embed", "embed-multilingual-v3.0");

    // Test with German text
    float[] embeddingDe = provider.embed("Was ist semantische Suche?", null);
    // Test with Spanish text
    float[] embeddingEs = provider.embed("Que es la busqueda semantica?", null);
    // Test with English text
    float[] embeddingEn = provider.embed("What is semantic search?", null);

    assertNotNull(embeddingDe, "German embedding should not be null");
    assertNotNull(embeddingEs, "Spanish embedding should not be null");
    assertNotNull(embeddingEn, "English embedding should not be null");

    // All three should be semantically similar (same meaning, different languages)
    double similarityDeEn = cosineSimilarity(embeddingDe, embeddingEn);
    double similarityEsEn = cosineSimilarity(embeddingEs, embeddingEn);

    System.out.println("Similarity (German-English): " + similarityDeEn);
    System.out.println("Similarity (Spanish-English): " + similarityEsEn);

    assertTrue(
        similarityDeEn > 0.7,
        "Same meaning in different languages should have reasonable similarity");
    assertTrue(
        similarityEsEn > 0.7,
        "Same meaning in different languages should have reasonable similarity");
  }

  /**
   * Compares our custom implementation against the official Cohere Java SDK to verify we get
   * identical embeddings for the same input.
   */
  @Test
  public void testMatchesOfficialCohereSDK() {
    String testText = "DataHub is a modern data catalog for the modern data stack.";
    String model = "embed-english-v3.0";

    // Generate embedding with our implementation
    CohereEmbeddingProvider ourProvider =
        new CohereEmbeddingProvider(apiKey, "https://api.cohere.ai/v1/embed", model);
    float[] ourEmbedding = ourProvider.embed(testText, null);

    // Generate embedding with official SDK
    Cohere sdkClient = Cohere.builder().token(apiKey).build();

    EmbedRequest request =
        EmbedRequest.builder()
            .texts(List.of(testText))
            .model(model)
            .inputType(EmbedInputType.SEARCH_QUERY)
            .build();

    EmbedResponse response = sdkClient.embed(request);

    // Extract float embeddings from SDK response
    EmbedFloatsResponse floatsResponse =
        response
            .getEmbeddingsFloats()
            .orElseThrow(() -> new RuntimeException("Expected float embeddings in response"));
    List<Double> sdkEmbeddingList = floatsResponse.getEmbeddings().get(0);

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

    // Note: Embedding APIs may not be perfectly deterministic - calling twice
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

    // Test embed-english-v3.0 (1024 dimensions)
    verifyDimensionsMatchSDK(testText, "embed-english-v3.0", 1024);

    // Test embed-multilingual-v3.0 (1024 dimensions)
    verifyDimensionsMatchSDK(testText, "embed-multilingual-v3.0", 1024);

    // Test embed-english-light-v3.0 (384 dimensions)
    verifyDimensionsMatchSDK(testText, "embed-english-light-v3.0", 384);
  }

  private void verifyDimensionsMatchSDK(String text, String modelName, int expectedDimensions) {
    // Our implementation
    CohereEmbeddingProvider ourProvider =
        new CohereEmbeddingProvider(apiKey, "https://api.cohere.ai/v1/embed", modelName);
    float[] ourEmbedding = ourProvider.embed(text, null);

    // SDK
    Cohere sdkClient = Cohere.builder().token(apiKey).build();
    EmbedRequest request =
        EmbedRequest.builder()
            .texts(List.of(text))
            .model(modelName)
            .inputType(EmbedInputType.SEARCH_QUERY)
            .build();

    EmbedResponse response = sdkClient.embed(request);
    EmbedFloatsResponse floatsResponse =
        response
            .getEmbeddingsFloats()
            .orElseThrow(() -> new RuntimeException("Expected float embeddings in response"));
    int sdkDimensions = floatsResponse.getEmbeddings().get(0).size();

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
