package com.linkedin.metadata.search.embedding;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Unit tests for {@link ClassicalEmbeddingProvider} (algorithm version hash-v1). */
public class ClassicalEmbeddingProviderTest {

  private static final int DIMS = 2048;

  private final ClassicalEmbeddingProvider provider =
      new ClassicalEmbeddingProvider("hash-v1-2048");

  /**
   * Golden vectors, identified by the lowercase hex SHA-256 over every component encoded as a
   * big-endian IEEE-754 binary32. The same inputs and digests are asserted by the Python ingestion
   * provider's test (test_classical_embedding_provider.py); the two lists MUST stay identical,
   * otherwise document vectors (Python) and query vectors (Java) silently diverge.
   */
  @DataProvider(name = "golden")
  public Object[][] golden() {
    return new Object[][] {
      {"Hello, World!", "bec441fc81b6a708326c70ddfd5229c197bc315c450ac4cda1bfeb63826ce9ab"},
      // ASCII fold: identical to the previous row
      {"hello, world!", "bec441fc81b6a708326c70ddfd5229c197bc315c450ac4cda1bfeb63826ce9ab"},
      {"user_id customer_id", "0a803a46b7b0db404eeeb79344d9d56effcb6d81cdbce71a4e488e5978c78ec6"},
      {"id", "91d865d24040d06fb4298bb4d51ecb47a60d919a74c11a97364aeb5ecaf6b2f3"},
      // "Grüße 東京 data"
      {
        "Gr\u00fc\u00dfe \u6771\u4eac data",
        "97777de6cfe5a291c9711b1be592479ea6867b80aca515d950a2018835b2dd74"
      },
      // "naïve café"
      {"na\u00efve caf\u00e9", "a8291f6e6798ef926025c47b85202c2734e66d025fa22b4bda60428104ae998e"},
      // e + combining acute accent
      {"e\u0301", "26e0cf186ad15cc6a307f215cd94ffd41d2115b7b021cbd21a56be33a8ae4c5e"},
      // precomposed é; must differ from the previous row
      {"\u00e9", "3376ecfc87f89b3d3d673b2ec8ce2645713dbcd47bd73b1c1af41d793c70cfbf"},
      // NBSP is not a separator: one word
      {"a\u00a0b", "6f3c34bb96eb1114f09ab6f8965df17a170225eecd2e1cd9b9636c213300151e"},
      // two one-character words: word features only, no n-grams
      {"x y", "7e9211ae1d54571d56bcda55c2b4b3595dcc7aa2b6aa4984943e6c12a9f90144"},
      // U+1F600 counts as one code point
      {"\uD83D\uDE00 emoji", "c8fd64833ba1b40ec97efb673c00e92218750f1f748da766f32355bc6e10e766"},
      // lone surrogate canonicalized to U+FFFD
      {"\uD800x", "3b34bccb1a43951ed6567c13bc809713f7082688395f8504b41537147a59b9bc"},
      {"a/b path", "cf00b52f191ebca9ac7dc94947f568176d943258f60275abbfe4edc9ab628de7"},
      // empty sentinel
      {"", "4fb362b7ae0cc6e8c1ee2ed26b3245c89937fa655d37f269ff3b1eb40db67033"},
      // whitespace only: empty sentinel
      {"   \t\n", "4fb362b7ae0cc6e8c1ee2ed26b3245c89937fa655d37f269ff3b1eb40db67033"},
    };
  }

  @Test(dataProvider = "golden")
  public void goldenVector(String input, String expectedDigest) throws Exception {
    float[] vector = provider.embed(input, null);

    assertEquals(vector.length, DIMS);
    assertTrue(Arrays.equals(vector, provider.embed(input, null)), "repeat must be bit-identical");
    boolean anyNonZero = false;
    for (float component : vector) {
      assertEquals(component, (float) (long) component, 0f, "components are integer-valued");
      anyNonZero |= component != 0f;
    }
    assertTrue(anyNonZero, "at least one component must be nonzero");
    assertEquals(digest(vector), expectedDigest, "golden digest mismatch for input: " + input);
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
