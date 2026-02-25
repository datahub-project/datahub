package com.linkedin.metadata.search.embedding;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.nio.charset.StandardCharsets;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

/** Unit tests for AwsBedrockEmbeddingProvider. */
public class AwsBedrockEmbeddingProviderTest {

  private BedrockRuntimeClient mockBedrockClient;
  private AwsBedrockEmbeddingProvider provider;

  @BeforeMethod
  public void setup() {
    mockBedrockClient = mock(BedrockRuntimeClient.class);
    provider = new AwsBedrockEmbeddingProvider(mockBedrockClient, "cohere.embed-english-v3", 2048);
  }

  @Test
  public void testEmbedSuccess() {
    // Mock Bedrock response with a simple embedding
    String responseJson =
        "{\"embeddings\": [[0.1, 0.2, 0.3]], \"id\": \"test-id\", \"response_type\": \"embeddings_floats\", \"texts\": [\"test query\"]}";
    InvokeModelResponse mockResponse =
        InvokeModelResponse.builder()
            .body(SdkBytes.fromString(responseJson, StandardCharsets.UTF_8))
            .build();

    when(mockBedrockClient.invokeModel(any(InvokeModelRequest.class))).thenReturn(mockResponse);

    // Test embedding generation
    float[] embedding = provider.embed("test query", null);

    // Verify results
    assertNotNull(embedding);
    assertEquals(embedding.length, 3);
    assertEquals(embedding[0], 0.1f, 0.001);
    assertEquals(embedding[1], 0.2f, 0.001);
    assertEquals(embedding[2], 0.3f, 0.001);

    // Verify the request was made
    verify(mockBedrockClient, times(1)).invokeModel(any(InvokeModelRequest.class));
  }

  @Test
  public void testEmbedWithCustomModel() {
    String responseJson =
        "{\"embeddings\": [[0.5, 0.6]], \"id\": \"test-id\", \"response_type\": \"embeddings_floats\", \"texts\": [\"test\"]}";
    InvokeModelResponse mockResponse =
        InvokeModelResponse.builder()
            .body(SdkBytes.fromString(responseJson, StandardCharsets.UTF_8))
            .build();

    when(mockBedrockClient.invokeModel(any(InvokeModelRequest.class))).thenReturn(mockResponse);

    // Test with custom model
    float[] embedding = provider.embed("test", "cohere.embed-multilingual-v3");

    assertNotNull(embedding);
    assertEquals(embedding.length, 2);
    verify(mockBedrockClient, times(1)).invokeModel(any(InvokeModelRequest.class));
  }

  @Test
  public void testEmbedWithLongText() {
    // Create text longer than max character length (2048)
    StringBuilder longText = new StringBuilder();
    for (int i = 0; i < 300; i++) {
      longText.append("test text ");
    }

    String responseJson =
        "{\"embeddings\": [[0.1, 0.2]], \"id\": \"test-id\", \"response_type\": \"embeddings_floats\", \"texts\": [\"truncated\"]}";
    InvokeModelResponse mockResponse =
        InvokeModelResponse.builder()
            .body(SdkBytes.fromString(responseJson, StandardCharsets.UTF_8))
            .build();

    when(mockBedrockClient.invokeModel(any(InvokeModelRequest.class))).thenReturn(mockResponse);

    // Should truncate to 2048 characters
    float[] embedding = provider.embed(longText.toString(), null);

    assertNotNull(embedding);
    assertEquals(embedding.length, 2);
    verify(mockBedrockClient, times(1)).invokeModel(any(InvokeModelRequest.class));
  }

  @Test
  public void testEmbedWith1024Dimensions() {
    // Mock realistic Cohere Embed v3 response with 1024 dimensions
    StringBuilder embeddingArray = new StringBuilder("[");
    for (int i = 0; i < 1024; i++) {
      if (i > 0) embeddingArray.append(", ");
      embeddingArray.append(String.format("%.6f", Math.random()));
    }
    embeddingArray.append("]");

    String responseJson =
        String.format(
            "{\"embeddings\": [%s], \"id\": \"test-id\", \"response_type\": \"embeddings_floats\", \"texts\": [\"test\"]}",
            embeddingArray);
    InvokeModelResponse mockResponse =
        InvokeModelResponse.builder()
            .body(SdkBytes.fromString(responseJson, StandardCharsets.UTF_8))
            .build();

    when(mockBedrockClient.invokeModel(any(InvokeModelRequest.class))).thenReturn(mockResponse);

    float[] embedding = provider.embed("test query", null);

    assertNotNull(embedding);
    assertEquals(embedding.length, 1024, "Cohere Embed v3 should return 1024 dimensions");
    verify(mockBedrockClient, times(1)).invokeModel(any(InvokeModelRequest.class));
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testEmbedWithMissingEmbeddings() {
    // Mock response without embeddings field
    String responseJson = "{\"id\": \"test-id\", \"response_type\": \"embeddings_floats\"}";
    InvokeModelResponse mockResponse =
        InvokeModelResponse.builder()
            .body(SdkBytes.fromString(responseJson, StandardCharsets.UTF_8))
            .build();

    when(mockBedrockClient.invokeModel(any(InvokeModelRequest.class))).thenReturn(mockResponse);

    provider.embed("test", null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testEmbedWithEmptyEmbeddingsArray() {
    // Mock response with empty embeddings array
    String responseJson =
        "{\"embeddings\": [], \"id\": \"test-id\", \"response_type\": \"embeddings_floats\"}";
    InvokeModelResponse mockResponse =
        InvokeModelResponse.builder()
            .body(SdkBytes.fromString(responseJson, StandardCharsets.UTF_8))
            .build();

    when(mockBedrockClient.invokeModel(any(InvokeModelRequest.class))).thenReturn(mockResponse);

    provider.embed("test", null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testEmbedWithInvalidJson() {
    // Mock response with invalid JSON
    String responseJson = "invalid json{{{";
    InvokeModelResponse mockResponse =
        InvokeModelResponse.builder()
            .body(SdkBytes.fromString(responseJson, StandardCharsets.UTF_8))
            .build();

    when(mockBedrockClient.invokeModel(any(InvokeModelRequest.class))).thenReturn(mockResponse);

    provider.embed("test", null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testEmbedWithNonNumericValues() {
    // Mock response with non-numeric embedding values
    String responseJson =
        "{\"embeddings\": [[\"invalid\", \"values\"]], \"id\": \"test-id\", \"response_type\": \"embeddings_floats\"}";
    InvokeModelResponse mockResponse =
        InvokeModelResponse.builder()
            .body(SdkBytes.fromString(responseJson, StandardCharsets.UTF_8))
            .build();

    when(mockBedrockClient.invokeModel(any(InvokeModelRequest.class))).thenReturn(mockResponse);

    provider.embed("test", null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testEmbedWithBedrockError() {
    // Mock Bedrock throwing an exception
    when(mockBedrockClient.invokeModel(any(InvokeModelRequest.class)))
        .thenThrow(new RuntimeException("Bedrock service error"));

    provider.embed("test", null);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testEmbedWithNullText() {
    provider.embed(null, null);
  }

  @Test
  public void testEmbedWithEmptyText() {
    String responseJson =
        "{\"embeddings\": [[0.1]], \"id\": \"test-id\", \"response_type\": \"embeddings_floats\", \"texts\": [\"\"]}";
    InvokeModelResponse mockResponse =
        InvokeModelResponse.builder()
            .body(SdkBytes.fromString(responseJson, StandardCharsets.UTF_8))
            .build();

    when(mockBedrockClient.invokeModel(any(InvokeModelRequest.class))).thenReturn(mockResponse);

    // Empty text should still work
    float[] embedding = provider.embed("", null);

    assertNotNull(embedding);
    verify(mockBedrockClient, times(1)).invokeModel(any(InvokeModelRequest.class));
  }

  @Test
  public void testConstructorWithRegion() {
    // Test that constructor with region string doesn't throw
    AwsBedrockEmbeddingProvider providerWithRegion = new AwsBedrockEmbeddingProvider("us-west-2");
    assertNotNull(providerWithRegion);
    providerWithRegion.close();
  }

  @Test
  public void testConstructorWithAllParameters() {
    // Test constructor with all parameters
    AwsBedrockEmbeddingProvider providerWithParams =
        new AwsBedrockEmbeddingProvider("us-east-1", "cohere.embed-multilingual-v3", 1024);
    assertNotNull(providerWithParams);
    providerWithParams.close();
  }

  @Test
  public void testClose() {
    // Test that close doesn't throw
    provider.close();
    verify(mockBedrockClient, times(1)).close();
  }

  @Test
  public void testMultipleEmbedCalls() {
    // Test multiple embed calls work correctly
    String responseJson1 =
        "{\"embeddings\": [[0.1, 0.2]], \"id\": \"test-id-1\", \"response_type\": \"embeddings_floats\", \"texts\": [\"first\"]}";
    String responseJson2 =
        "{\"embeddings\": [[0.3, 0.4]], \"id\": \"test-id-2\", \"response_type\": \"embeddings_floats\", \"texts\": [\"second\"]}";

    InvokeModelResponse mockResponse1 =
        InvokeModelResponse.builder()
            .body(SdkBytes.fromString(responseJson1, StandardCharsets.UTF_8))
            .build();
    InvokeModelResponse mockResponse2 =
        InvokeModelResponse.builder()
            .body(SdkBytes.fromString(responseJson2, StandardCharsets.UTF_8))
            .build();

    when(mockBedrockClient.invokeModel(any(InvokeModelRequest.class)))
        .thenReturn(mockResponse1)
        .thenReturn(mockResponse2);

    float[] embedding1 = provider.embed("first query", null);
    float[] embedding2 = provider.embed("second query", null);

    assertNotNull(embedding1);
    assertNotNull(embedding2);
    assertEquals(embedding1[0], 0.1f, 0.001);
    assertEquals(embedding2[0], 0.3f, 0.001);
    verify(mockBedrockClient, times(2)).invokeModel(any(InvokeModelRequest.class));
  }
}
