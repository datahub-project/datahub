package com.linkedin.metadata.search.embedding;

import ai.djl.huggingface.tokenizers.Encoding;
import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import java.nio.LongBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Embedding provider that runs ONNX model inference entirely in-process, eliminating the need for
 * an external embedding server like Ollama. Uses ONNX Runtime for inference and DJL's HuggingFace
 * tokenizer binding for tokenization.
 *
 * <p>Thread safety: this class is safe for concurrent use. {@link OrtSession#run} and {@link
 * HuggingFaceTokenizer#encode} are both documented as thread-safe. Each {@link #embed} call
 * allocates its own tensors in try-with-resources and shares no mutable state.
 *
 * <p>The model directory must contain:
 *
 * <ul>
 *   <li>{@code model.onnx} (or {@code model_quantized.onnx}) — the ONNX model file
 *   <li>{@code tokenizer.json} — HuggingFace tokenizer configuration
 * </ul>
 *
 * <p>Candidate models (download separately):
 *
 * <ul>
 *   <li><b>Snowflake Arctic-embed-s</b>: 33M params, ~130 MB, 384 dims
 *   <li><b>Snowflake Arctic-embed-l</b>: 137M params, ~550 MB, 1024 dims
 *   <li><b>BGE-base-en-v1.5</b>: 109M params, ~438 MB, 768 dims
 * </ul>
 */
@Slf4j
public class OnnxEmbeddingProvider implements EmbeddingProvider {

  private final OrtEnvironment ortEnvironment;
  private final OrtSession ortSession;
  private final HuggingFaceTokenizer tokenizer;
  @Nonnull private final String modelName;
  private final boolean supportsTokenTypeIds;
  private final int maxCharacterLength;
  private final int outputDimension;

  /**
   * Creates a provider by loading model and tokenizer from a directory on disk.
   *
   * @param modelDir path to directory containing model.onnx and tokenizer.json
   * @param intraOpThreads number of threads for ONNX intra-op parallelism (0 = default)
   * @param maxCharacterLength maximum text length before truncation (0 = no limit)
   */
  public OnnxEmbeddingProvider(@Nonnull Path modelDir, int intraOpThreads, int maxCharacterLength) {
    Objects.requireNonNull(modelDir, "modelDir cannot be null");

    if (!Files.isDirectory(modelDir)) {
      throw new IllegalArgumentException("Model directory does not exist: " + modelDir);
    }

    Path modelFile = resolveModelFile(modelDir);
    Path tokenizerFile = modelDir.resolve("tokenizer.json");
    if (!Files.isRegularFile(tokenizerFile)) {
      throw new IllegalArgumentException(
          "tokenizer.json not found in model directory: " + modelDir);
    }

    this.modelName = modelDir.getFileName().toString();
    this.maxCharacterLength = maxCharacterLength;

    OrtSession localSession = null;
    HuggingFaceTokenizer localTokenizer = null;
    try {
      long modelSizeBytes = Files.size(modelFile);
      log.info(
          "Loading ONNX model from {} ({} MB). This may take 10-60 seconds for large models...",
          modelFile.getFileName(),
          modelSizeBytes / (1024 * 1024));

      long loadStartNanos = System.nanoTime();

      this.ortEnvironment = OrtEnvironment.getEnvironment();
      try (OrtSession.SessionOptions options = new OrtSession.SessionOptions()) {
        if (intraOpThreads > 0) {
          options.setIntraOpNumThreads(intraOpThreads);
        }
        localSession = ortEnvironment.createSession(modelFile.toString(), options);
      }
      localTokenizer = HuggingFaceTokenizer.newInstance(tokenizerFile);
      this.ortSession = localSession;
      this.tokenizer = localTokenizer;
      this.supportsTokenTypeIds = ortSession.getInputNames().contains("token_type_ids");

      long loadElapsedMs = (System.nanoTime() - loadStartNanos) / 1_000_000;

      // Probe to determine output dimension and validate model works
      float[] probe = embedInternal("dimension probe");
      this.outputDimension = probe.length;

      log.info(
          "Initialized OnnxEmbeddingProvider in {}ms: model={}, file={}, outputDimension={}, "
              + "inputs={}, outputs={}, intraOpThreads={}",
          loadElapsedMs,
          modelName,
          modelFile.getFileName(),
          outputDimension,
          ortSession.getInputNames(),
          ortSession.getOutputNames(),
          intraOpThreads > 0 ? intraOpThreads : "default");
    } catch (Exception e) {
      if (localTokenizer != null) {
        localTokenizer.close();
      }
      if (localSession != null) {
        try {
          localSession.close();
        } catch (OrtException closeEx) {
          e.addSuppressed(closeEx);
        }
      }
      if (e instanceof RuntimeException re) {
        throw re;
      }
      throw new RuntimeException(
          "Failed to initialize ONNX embedding provider from " + modelDir, e);
    }
  }

  /**
   * Test-friendly constructor that accepts pre-built session and tokenizer.
   *
   * @param ortEnvironment ONNX Runtime environment
   * @param ortSession pre-loaded ONNX session
   * @param tokenizer pre-loaded tokenizer
   * @param modelName display name for logging
   * @param maxCharacterLength maximum text length before truncation (0 = no limit)
   */
  OnnxEmbeddingProvider(
      @Nonnull OrtEnvironment ortEnvironment,
      @Nonnull OrtSession ortSession,
      @Nonnull HuggingFaceTokenizer tokenizer,
      @Nonnull String modelName,
      int maxCharacterLength) {
    this.ortEnvironment = Objects.requireNonNull(ortEnvironment, "ortEnvironment cannot be null");
    this.ortSession = Objects.requireNonNull(ortSession, "ortSession cannot be null");
    this.tokenizer = Objects.requireNonNull(tokenizer, "tokenizer cannot be null");
    this.modelName = Objects.requireNonNull(modelName, "modelName cannot be null");
    this.supportsTokenTypeIds = ortSession.getInputNames().contains("token_type_ids");
    this.maxCharacterLength = maxCharacterLength;
    this.outputDimension = -1; // not determined in test constructor

    log.info("Initialized OnnxEmbeddingProvider (injected): model={}", modelName);
  }

  /** Returns the embedding output dimension, determined by a probe inference at startup. */
  public int getOutputDimension() {
    return outputDimension;
  }

  @Override
  @Nonnull
  public float[] embed(@Nonnull String text, @Nullable String model) {
    Objects.requireNonNull(text, "text cannot be null");

    if (model != null && !model.isBlank() && !model.equals(modelName)) {
      log.warn(
          "ONNX provider ignores the model parameter. Requested '{}' but using loaded model '{}'.",
          model,
          modelName);
    }

    String input = text;
    if (maxCharacterLength > 0 && input.length() > maxCharacterLength) {
      input = input.substring(0, maxCharacterLength);
    }

    long startNanos = System.nanoTime();
    try {
      float[] result = embedInternal(input);
      long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
      log.debug("ONNX embed for model {} completed in {}ms", modelName, elapsedMs);
      return result;
    } catch (OrtException e) {
      throw new RuntimeException(
          String.format("ONNX inference failed for model %s: %s", modelName, e.getMessage()), e);
    }
  }

  private float[] embedInternal(String text) throws OrtException {
    Encoding encoding = tokenizer.encode(text);
    long[] inputIds = encoding.getIds();
    long[] attentionMask = encoding.getAttentionMask();
    long[] tokenTypeIds = encoding.getTypeIds();

    long[] shape = new long[] {1, inputIds.length};

    try (OnnxTensor inputIdsTensor =
            OnnxTensor.createTensor(ortEnvironment, LongBuffer.wrap(inputIds), shape);
        OnnxTensor attentionMaskTensor =
            OnnxTensor.createTensor(ortEnvironment, LongBuffer.wrap(attentionMask), shape);
        OnnxTensor tokenTypeIdsTensor =
            OnnxTensor.createTensor(ortEnvironment, LongBuffer.wrap(tokenTypeIds), shape)) {

      Map<String, OnnxTensor> inputs =
          buildInputMap(inputIdsTensor, attentionMaskTensor, tokenTypeIdsTensor);

      try (OrtSession.Result result = ortSession.run(inputs)) {
        return extractEmbedding(result, attentionMask);
      }
    }
  }

  private Map<String, OnnxTensor> buildInputMap(
      OnnxTensor inputIds, OnnxTensor attentionMask, OnnxTensor tokenTypeIds) {
    var builder =
        new HashMap<String, OnnxTensor>(
            Map.of("input_ids", inputIds, "attention_mask", attentionMask));

    if (supportsTokenTypeIds) {
      builder.put("token_type_ids", tokenTypeIds);
    }
    return builder;
  }

  /**
   * Extracts the embedding from ONNX output. Most sentence-transformer models output a [batch_size,
   * seq_len, hidden_dim] tensor (last_hidden_state). We perform mean pooling over the token
   * dimension, masked by attention_mask, to produce a single [hidden_dim] vector.
   *
   * <p>Some models output [batch_size, hidden_dim] directly (sentence_embedding or pooler_output);
   * in that case we use it as-is.
   */
  private float[] extractEmbedding(OrtSession.Result result, long[] attentionMask)
      throws OrtException {
    if (result.size() == 0) {
      throw new RuntimeException("ONNX model '" + modelName + "' produced no output tensors");
    }
    Object rawOutput = result.get(0).getValue();
    if (rawOutput == null) {
      throw new RuntimeException("ONNX model '" + modelName + "' produced a null output value");
    }

    if (rawOutput instanceof float[][]) {
      float[][] pooled = (float[][]) rawOutput;
      return pooled[0];
    }

    if (rawOutput instanceof float[][][]) {
      float[][][] tokenEmbeddings = (float[][][]) rawOutput;
      return meanPool(tokenEmbeddings[0], attentionMask);
    }

    throw new RuntimeException(
        "Unexpected ONNX output shape for model "
            + modelName
            + ". Expected float[][] or float[][][], got: "
            + rawOutput.getClass().getSimpleName());
  }

  /** Mean pooling: average token embeddings weighted by attention mask. */
  private float[] meanPool(float[][] tokenEmbeddings, long[] attentionMask) {
    if (tokenEmbeddings.length == 0) {
      throw new RuntimeException(
          "ONNX model '" + modelName + "' returned empty token embeddings (seq_len=0)");
    }
    int hiddenDim = tokenEmbeddings[0].length;
    float[] summed = new float[hiddenDim];
    float maskSum = 0;

    int seqLen = Math.min(tokenEmbeddings.length, attentionMask.length);
    for (int t = 0; t < seqLen; t++) {
      float mask = attentionMask[t];
      if (mask == 0) continue;
      maskSum += mask;
      for (int d = 0; d < hiddenDim; d++) {
        summed[d] += tokenEmbeddings[t][d] * mask;
      }
    }

    if (maskSum == 0) {
      throw new RuntimeException(
          "All tokens masked — cannot produce embedding for model " + modelName);
    }

    for (int d = 0; d < hiddenDim; d++) {
      summed[d] /= maskSum;
    }
    return summed;
  }

  private static Path resolveModelFile(Path modelDir) {
    Path quantized = modelDir.resolve("model_quantized.onnx");
    if (Files.isRegularFile(quantized)) {
      return quantized;
    }
    Path standard = modelDir.resolve("model.onnx");
    if (Files.isRegularFile(standard)) {
      return standard;
    }
    throw new IllegalArgumentException(
        "No model.onnx or model_quantized.onnx found in: " + modelDir);
  }

  @Override
  public void close() {
    try {
      ortSession.close();
    } catch (Exception e) {
      log.error(
          "Error closing ONNX session for model '{}' — native memory may be leaked", modelName, e);
    }
    try {
      tokenizer.close();
    } catch (RuntimeException e) {
      log.error(
          "Error closing tokenizer for model '{}' — native memory may be leaked", modelName, e);
    }
  }
}
