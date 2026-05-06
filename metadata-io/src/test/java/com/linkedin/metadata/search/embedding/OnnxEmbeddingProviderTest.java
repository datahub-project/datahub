package com.linkedin.metadata.search.embedding;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import ai.djl.huggingface.tokenizers.Encoding;
import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import ai.onnxruntime.OnnxValue;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import java.nio.file.Path;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Unit tests for OnnxEmbeddingProvider. */
public class OnnxEmbeddingProviderTest {

  private static final int MAX_CHAR_LENGTH = 2048;

  private OrtEnvironment ortEnvironment;
  private OrtSession mockSession;
  private HuggingFaceTokenizer mockTokenizer;
  private OnnxEmbeddingProvider provider;

  @BeforeMethod
  public void setup() {
    ortEnvironment = OrtEnvironment.getEnvironment();
    mockSession = mock(OrtSession.class);
    mockTokenizer = mock(HuggingFaceTokenizer.class);
    when(mockSession.getInputNames())
        .thenReturn(Set.of("input_ids", "attention_mask", "token_type_ids"));
    provider =
        new OnnxEmbeddingProvider(
            ortEnvironment, mockSession, mockTokenizer, "test-model", MAX_CHAR_LENGTH);
  }

  @Test
  public void testEmbedSuccessWithPooledOutput() throws Exception {
    float[][] pooledOutput = {{0.1f, 0.2f, 0.3f}};

    setupMocks(new long[] {101, 2023, 102}, new long[] {1, 1, 1}, new long[] {0, 0, 0});
    setupSessionResult(pooledOutput);

    float[] embedding = provider.embed("test query", null);

    assertNotNull(embedding);
    assertEquals(embedding.length, 3);
    assertEquals(embedding[0], 0.1f, 0.001);
    assertEquals(embedding[1], 0.2f, 0.001);
    assertEquals(embedding[2], 0.3f, 0.001);
  }

  @Test
  public void testEmbedSuccessWithTokenLevelOutput() throws Exception {
    float[][][] tokenOutput = {{{0.2f, 0.4f}, {0.4f, 0.6f}, {0.6f, 0.8f}}};

    setupMocks(new long[] {101, 2023, 102}, new long[] {1, 1, 1}, new long[] {0, 0, 0});
    setupSessionResult(tokenOutput);

    float[] embedding = provider.embed("test query", null);

    assertNotNull(embedding);
    assertEquals(embedding.length, 2);
    assertEquals(embedding[0], 0.4f, 0.001);
    assertEquals(embedding[1], 0.6f, 0.001);
  }

  @Test
  public void testMeanPoolingRespectsAttentionMask() throws Exception {
    float[][][] tokenOutput = {{{0.3f, 0.6f}, {0.6f, 0.9f}, {0.9f, 1.2f}, {99f, 99f}}};

    setupMocks(new long[] {101, 2023, 102, 0}, new long[] {1, 1, 1, 0}, new long[] {0, 0, 0, 0});
    setupSessionResult(tokenOutput);

    float[] embedding = provider.embed("padded text", null);

    assertNotNull(embedding);
    assertEquals(embedding.length, 2);
    assertEquals(embedding[0], 0.6f, 0.001);
    assertEquals(embedding[1], 0.9f, 0.001);
  }

  @Test
  public void testEmbedWith384Dimensions() throws Exception {
    float[][] pooledOutput = new float[1][384];
    for (int i = 0; i < 384; i++) {
      pooledOutput[0][i] = (float) (i * 0.001);
    }

    setupMocks(new long[] {101, 102}, new long[] {1, 1}, new long[] {0, 0});
    setupSessionResult(pooledOutput);

    float[] embedding = provider.embed("test", null);

    assertNotNull(embedding);
    assertEquals(embedding.length, 384);
    assertEquals(embedding[0], 0.0f, 0.001);
    assertEquals(embedding[383], 0.383f, 0.001);
  }

  @Test
  public void testEmbedIgnoresModelParameter() throws Exception {
    float[][] pooledOutput = {{0.5f}};

    setupMocks(new long[] {101, 102}, new long[] {1, 1}, new long[] {0, 0});
    setupSessionResult(pooledOutput);

    float[] embedding = provider.embed("text", "some-other-model");

    assertNotNull(embedding);
    assertEquals(embedding.length, 1);
    assertEquals(embedding[0], 0.5f, 0.001);
  }

  @Test
  public void testEmbedWithEmptyText() throws Exception {
    float[][] pooledOutput = {{0.1f, 0.2f}};

    setupMocks(new long[] {101, 102}, new long[] {1, 1}, new long[] {0, 0});
    setupSessionResult(pooledOutput);

    float[] embedding = provider.embed("", null);

    assertNotNull(embedding);
    assertEquals(embedding.length, 2);
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*ONNX inference failed.*")
  public void testEmbedWithOrtException() throws Exception {
    setupMocks(new long[] {101, 102}, new long[] {1, 1}, new long[] {0, 0});
    when(mockSession.run(any())).thenThrow(new OrtException("Session error"));

    provider.embed("test", null);
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*Unexpected ONNX output shape.*")
  public void testEmbedWithUnexpectedOutputShape() throws Exception {
    setupMocks(new long[] {101, 102}, new long[] {1, 1}, new long[] {0, 0});
    setupSessionResult(new float[] {0.1f, 0.2f});

    provider.embed("test", null);
  }

  @Test
  public void testTokenTypeIdsOptional() throws Exception {
    OrtSession noTokenTypeSession = mock(OrtSession.class);
    when(noTokenTypeSession.getInputNames()).thenReturn(Set.of("input_ids", "attention_mask"));
    OnnxEmbeddingProvider noTokenTypeProvider =
        new OnnxEmbeddingProvider(
            ortEnvironment, noTokenTypeSession, mockTokenizer, "no-ttype", MAX_CHAR_LENGTH);

    float[][] pooledOutput = {{0.1f}};
    setupMocks(new long[] {101, 102}, new long[] {1, 1}, new long[] {0, 0});

    OrtSession.Result mockResult = createMockResult(pooledOutput);
    when(noTokenTypeSession.run(any())).thenReturn(mockResult);

    float[] embedding = noTokenTypeProvider.embed("test", null);

    assertNotNull(embedding);
    assertEquals(embedding.length, 1);
  }

  @Test
  public void testMultipleEmbedCalls() throws Exception {
    float[][] output1 = {{0.1f, 0.2f}};
    float[][] output2 = {{0.3f, 0.4f}};

    setupMocks(new long[] {101, 102}, new long[] {1, 1}, new long[] {0, 0});

    OrtSession.Result mockResult1 = createMockResult(output1);
    OrtSession.Result mockResult2 = createMockResult(output2);
    when(mockSession.run(any())).thenReturn(mockResult1).thenReturn(mockResult2);

    float[] embedding1 = provider.embed("first", null);
    float[] embedding2 = provider.embed("second", null);

    assertEquals(embedding1[0], 0.1f, 0.001);
    assertEquals(embedding2[0], 0.3f, 0.001);
    verify(mockSession, times(2)).run(any());
  }

  @Test(
      expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*does not exist.*")
  public void testConstructorWithNonexistentDirectory() {
    new OnnxEmbeddingProvider(Path.of("/nonexistent/model/dir"), 0, 0);
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*All tokens masked.*")
  public void testMeanPoolingWithAllZeroMask() throws Exception {
    float[][][] tokenOutput = {{{0.1f, 0.2f}, {0.3f, 0.4f}}};

    setupMocks(new long[] {101, 102}, new long[] {0, 0}, new long[] {0, 0});
    setupSessionResult(tokenOutput);

    provider.embed("test", null);
  }

  @Test
  public void testMaxCharacterLengthTruncation() throws Exception {
    OnnxEmbeddingProvider shortProvider =
        new OnnxEmbeddingProvider(ortEnvironment, mockSession, mockTokenizer, "short-model", 10);

    float[][] pooledOutput = {{0.5f}};
    setupMocks(new long[] {101, 102}, new long[] {1, 1}, new long[] {0, 0});
    setupSessionResult(pooledOutput);

    shortProvider.embed("this text is longer than ten characters", null);

    // Verify that the tokenizer received truncated text
    verify(mockTokenizer).encode("this text ");
  }

  @Test
  public void testNoTruncationWhenBelowLimit() throws Exception {
    float[][] pooledOutput = {{0.5f}};
    setupMocks(new long[] {101, 102}, new long[] {1, 1}, new long[] {0, 0});
    setupSessionResult(pooledOutput);

    provider.embed("short", null);

    verify(mockTokenizer).encode("short");
  }

  // --- Helper methods ---

  private void setupMocks(long[] inputIds, long[] attentionMask, long[] tokenTypeIds) {
    Encoding mockEncoding = mock(Encoding.class);
    when(mockEncoding.getIds()).thenReturn(inputIds);
    when(mockEncoding.getAttentionMask()).thenReturn(attentionMask);
    when(mockEncoding.getTypeIds()).thenReturn(tokenTypeIds);
    when(mockTokenizer.encode(any(String.class))).thenReturn(mockEncoding);
  }

  private void setupSessionResult(Object outputValue) throws OrtException {
    OrtSession.Result mockResult = createMockResult(outputValue);
    when(mockSession.run(any())).thenReturn(mockResult);
  }

  @SuppressWarnings("unchecked")
  private OrtSession.Result createMockResult(Object outputValue) throws OrtException {
    OrtSession.Result mockResult = mock(OrtSession.Result.class);
    OnnxValue mockOnnxValue = mock(OnnxValue.class);
    when(mockOnnxValue.getValue()).thenReturn(outputValue);
    when(mockResult.size()).thenReturn(1);
    when(mockResult.get(0)).thenReturn(mockOnnxValue);
    return mockResult;
  }
}
