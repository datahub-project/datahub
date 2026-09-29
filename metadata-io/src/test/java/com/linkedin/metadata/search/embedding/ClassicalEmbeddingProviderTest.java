package com.linkedin.metadata.search.embedding;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Unit tests for {@link ClassicalEmbeddingProvider} (algorithm version hash-v1). */
public class ClassicalEmbeddingProviderTest {

  private static final int DIMS = 2048;

  /**
   * Golden vectors shared with the Python ingestion provider's test
   * (test_classical_embedding_provider.py reads the same file), identified by the lowercase hex
   * SHA-256 over every component encoded as a big-endian IEEE-754 binary32. One fixture for both
   * suites: a digest that changes on one side without the other means document vectors (Python) and
   * query vectors (Java) have diverged.
   */
  static final String GOLDEN_RESOURCE = "/embedding/classical_hash_v1_2048_golden.json";

  private final ClassicalEmbeddingProvider provider =
      new ClassicalEmbeddingProvider("hash-v1-2048");

  @DataProvider(name = "golden")
  public Object[][] golden() throws Exception {
    List<Object[]> rows = new ArrayList<>();
    try (InputStream in = getClass().getResourceAsStream(GOLDEN_RESOURCE)) {
      for (JsonNode row : new ObjectMapper().readTree(in)) {
        rows.add(
            new Object[] {
              row.get("id").asText(), row.get("text").asText(), row.get("sha256").asText()
            });
      }
    }
    assertFalse(rows.isEmpty(), "golden fixture is empty: " + GOLDEN_RESOURCE);
    return rows.toArray(new Object[0][]);
  }

  @Test(dataProvider = "golden")
  public void goldenVector(String id, String input, String expectedDigest) throws Exception {
    float[] vector = provider.embed(input, null);

    assertEquals(vector.length, DIMS);
    assertTrue(Arrays.equals(vector, provider.embed(input, null)), "repeat must be bit-identical");
    boolean anyNonZero = false;
    for (float component : vector) {
      assertEquals(component, (float) (long) component, 0f, "components are integer-valued");
      anyNonZero |= component != 0f;
    }
    assertTrue(anyNonZero, "at least one component must be nonzero");
    assertEquals(digest(vector), expectedDigest, "golden digest mismatch for row: " + id);
  }

  @Test
  public void asciiCaseFoldOnly() {
    assertTrue(
        Arrays.equals(
            provider.embed("Hello, World!", null), provider.embed("hello, world!", null)));
    // Only A-Z fold: precomposed vs combining-mark forms stay different (documented limitation).
    assertFalse(Arrays.equals(provider.embed("e\u0301", null), provider.embed("\u00e9", null)));
  }

  @Test
  public void nbspAndNulAreNotSeparators() {
    assertFalse(Arrays.equals(provider.embed("x\u0000y", null), provider.embed("x y", null)));
    assertFalse(Arrays.equals(provider.embed("a\u00a0b", null), provider.embed("a b", null)));
  }

  @Test
  public void loneSurrogateIsCanonicalizedToReplacementChar() {
    assertTrue(Arrays.equals(provider.embed("\uD800x", null), provider.embed("\uFFFDx", null)));
  }

  @Test
  public void emptyAndWhitespaceOnlyGiveTheSentinel() {
    for (String input : new String[] {"", "   \t\n"}) {
      float[] vector = provider.embed(input, null);
      assertEquals(vector[0], 1.0f, 0f);
      for (int i = 1; i < vector.length; i++) {
        assertEquals(vector[i], 0f, 0f);
      }
    }
  }

  @Test
  public void inputLimitIsCountedInCodePoints() {
    // 16384 supplementary characters are 32768 UTF-16 chars but exactly 16384 code points.
    assertEquals(provider.embed("\uD83D\uDE00".repeat(16384), null).length, DIMS);
    assertThrows(IllegalArgumentException.class, () -> provider.embed("a".repeat(16385), null));
  }

  @Test
  public void modelArgumentIsIgnored() {
    assertTrue(Arrays.equals(provider.embed("id", null), provider.embed("id", "anything")));
  }

  @Test
  public void modelNameFixesTheDimensions() {
    ClassicalEmbeddingProvider small = new ClassicalEmbeddingProvider("hash-v1-64");
    assertEquals(small.getDimensions(), 64);
    assertEquals(small.embed("user_id", null).length, 64);
    assertEquals(new ClassicalEmbeddingProvider("hash-v1-4096").getDimensions(), 4096);
  }

  @Test
  public void rejectsMalformedModelNames() {
    for (String model :
        new String[] {
          "hash-v1-0", "hash-v1-4097", "hash-v2-2048", "HASH-V1-2048", "hash-v1-02048"
        }) {
      assertThrows(IllegalArgumentException.class, () -> new ClassicalEmbeddingProvider(model));
    }
  }

  private static String digest(float[] vector) throws Exception {
    ByteBuffer buffer = ByteBuffer.allocate(vector.length * Float.BYTES);
    for (float component : vector) {
      buffer.putFloat(component);
    }
    return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(buffer.array()));
  }
}
