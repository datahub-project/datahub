package com.linkedin.metadata.search.embedding;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Deterministic, dependency-free embedding provider. Text is reduced to word unigrams plus
 * boundary-marked character bigrams and trigrams, every distinct feature is SHA-256 hashed into one
 * of {@code dims} signed buckets, and the integer bucket sums are returned as the vector. No API
 * key, endpoint, or model download is needed, and the same text always yields the same vector.
 *
 * <p>The Python ingestion provider implements the identical {@code hash-v1} algorithm, so document
 * and query vectors are bit-identical for the same text. Any change to the algorithm below is a new
 * version and must ship under a new model name ({@code hash-v2-...}), never as an edit to v1:
 * stored v1 document vectors would silently stop matching v1 query vectors.
 *
 * <p>Matching is lexical, not semantic. The provider is stateless, so there is no IDF or other
 * corpus weighting, and no synonym or paraphrase matching. Vectors are unnormalized integer counts,
 * so the index must use a cosine space type.
 *
 * <p>Known parity caveat: a string holding an adjacent high+low surrogate pair as two code units
 * (only reachable via surrogatepass/surrogateescape decoding in Python) hashes as two U+FFFD in
 * Python but as one supplementary code point in Java; JSON ingress on both sides cannot produce it.
 *
 * <p>Model name format: {@code hash-v1-<dims>} with dims in 1..{@value #MAX_DIMENSIONS}. Safe for
 * concurrent use: each call owns its own state.
 */
public class ClassicalEmbeddingProvider implements EmbeddingProvider {

  /** Longer inputs are rejected, never truncated: truncation would break query/document parity. */
  public static final int MAX_CODE_POINTS = 16384;

  public static final int MAX_DIMENSIONS = 4096;

  private static final Pattern MODEL_PATTERN = Pattern.compile("^hash-v1-([1-9][0-9]*)$");

  /**
   * Feature bytes are tag-framed so text can never alias a feature type: a word hashes as {@code
   * 0x01 0x03 utf8(word)}; a k-gram (k = 2 or 3) as {@code k, boundary, utf8(gram)} with boundary
   * bit 1 = starts the word, bit 2 = ends the word.
   */
  private static final String WORD_KEY_PREFIX = "" + (char) 0x01 + (char) 0x03;

  private final int dimensions;

  public ClassicalEmbeddingProvider(@Nonnull String modelName) {
    Matcher matcher = MODEL_PATTERN.matcher(modelName);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          "Unsupported classical embedding model '"
              + modelName
              + "'. Expected hash-v1-<dims> with dims in 1.."
              + MAX_DIMENSIONS
              + ".");
    }
    String digits = matcher.group(1);
    int dims = digits.length() > 4 ? Integer.MAX_VALUE : Integer.parseInt(digits);
    if (dims > MAX_DIMENSIONS) {
      throw new IllegalArgumentException(
          "Classical embedding model '"
              + modelName
              + "' exceeds the maximum of "
              + MAX_DIMENSIONS
              + " dimensions.");
    }
    this.dimensions = dims;
  }

  public int getDimensions() {
    return dimensions;
  }

  /** {@code model} is ignored: algorithm and width are fixed by the constructor's model name. */
  @Nonnull
  @Override
  public float[] embed(@Nonnull String text, @Nullable String model) {
    // Reject before materializing the code points so oversized input cannot force a large
    // temporary allocation.
    int codePointCount = text.codePointCount(0, text.length());
    if (codePointCount > MAX_CODE_POINTS) {
      throw new IllegalArgumentException(
          "Text has "
              + codePointCount
              + " code points; the classical embedding provider accepts at most "
              + MAX_CODE_POINTS
              + ".");
    }
    int[] cps = text.codePoints().toArray();
    for (int i = 0; i < cps.length; i++) {
      int cp = cps[i];
      if (cp >= 0xD800 && cp <= 0xDFFF) {
        // Lone surrogate: Python cannot UTF-8 encode it, so both sides canonicalize to U+FFFD.
        cps[i] = 0xFFFD;
      } else if (cp >= 'A' && cp <= 'Z') {
        // ASCII-only fold; no Unicode case folding or normalization on either side.
        cps[i] = cp + 32;
      }
    }

    // A feature key is its feature bytes decoded as UTF-8 (tag and boundary chars are below 0x80,
    // grams contain no lone surrogates), so key equality is byte equality and the key's UTF-8
    // encoding is exactly what gets hashed.
    Map<String, Integer> termFrequency = new HashMap<>();
    StringBuilder joinedWords = new StringBuilder();
    int i = 0;
    while (i < cps.length) {
      if (isSeparator(cps[i])) {
        i++;
        continue;
      }
      int start = i;
      while (i < cps.length && !isSeparator(cps[i])) {
        i++;
      }
      int n = i - start;
      String word = new String(cps, start, n);
      if (joinedWords.length() > 0) {
        joinedWords.append(' ');
      }
      joinedWords.append(word);
      termFrequency.merge(WORD_KEY_PREFIX + word, 1, Integer::sum);
      for (int k = 2; k <= 3; k++) {
        for (int j = 0; j + k <= n; j++) {
          int boundary = (j == 0 ? 1 : 0) | (j + k == n ? 2 : 0);
          termFrequency.merge(
              "" + (char) k + (char) boundary + new String(cps, start + j, k), 1, Integer::sum);
        }
      }
    }

    long[] acc = new long[dimensions];
    if (joinedWords.length() == 0) {
      // Empty sentinel: cosine similarity rejects zero-magnitude vectors.
      acc[0] = 1;
    } else {
      MessageDigest sha256 = sha256();
      for (Map.Entry<String, Integer> feature : termFrequency.entrySet()) {
        byte[] digest = sha256.digest(feature.getKey().getBytes(StandardCharsets.UTF_8));
        long weight = ceilSqrt(feature.getValue());
        acc[bucket(digest)] += (digest[4] & 1) == 0 ? weight : -weight;
      }
      boolean allZero = true;
      for (long value : acc) {
        if (value != 0) {
          allZero = false;
          break;
        }
      }
      if (allZero) {
        // Full cancellation: pick a deterministic bucket from the whole normalized text rather
        // than collapsing onto the empty sentinel.
        byte[] words = joinedWords.toString().getBytes(StandardCharsets.UTF_8);
        byte[] fallback = new byte[words.length + 1];
        System.arraycopy(words, 0, fallback, 1, words.length);
        acc[bucket(sha256.digest(fallback))] = 1;
      }
    }

    // Exact conversion: the input cap keeps |acc[i]| far below 2^24.
    float[] vector = new float[dimensions];
    for (int d = 0; d < dimensions; d++) {
      vector[d] = (float) acc[d];
    }
    return vector;
  }

  private int bucket(byte[] digest) {
    long unsigned = Integer.toUnsignedLong(ByteBuffer.wrap(digest).getInt());
    return (int) (unsigned % dimensions);
  }

  private static boolean isSeparator(int cp) {
    return cp == 0x20 || (cp >= 0x09 && cp <= 0x0D);
  }

  /** Smallest k with k*k >= n, settled with integer comparisons so both languages agree. */
  private static long ceilSqrt(long n) {
    long k = (long) Math.sqrt((double) n);
    while (k * k < n) {
      k++;
    }
    while (k > 0 && (k - 1) * (k - 1) >= n) {
      k--;
    }
    return k;
  }

  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is mandatory for every JVM", e);
    }
  }
}
