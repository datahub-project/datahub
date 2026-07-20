package com.linkedin.metadata.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.CompressionType;
import com.linkedin.common.LargeString;
import org.testng.annotations.Test;

public class LargeStringsTest {

  @Test
  public void testSmallTextStaysUncompressed() {
    final String text = "small contract text";
    final LargeString encoded = LargeStrings.encode(text);
    assertEquals(encoded.getCompression(), CompressionType.NONE);
    assertEquals(encoded.getBlob(), text);
    assertEquals(
        (long) encoded.getUncompressedSize(),
        text.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
    assertEquals(LargeStrings.decode(encoded), text);
  }

  @Test
  public void testLargeTextCompressesAndRoundTrips() {
    final StringBuilder sb = new StringBuilder();
    while (sb.length() <= LargeStrings.DEFAULT_THRESHOLD_BYTES) {
      sb.append("the quick brown fox jumps over the lazy dog\n");
    }
    final String text = sb.toString();
    final LargeString encoded = LargeStrings.encode(text);
    assertEquals(encoded.getCompression(), CompressionType.GZIP);
    assertEquals(LargeStrings.decode(encoded), text);
  }

  @Test
  public void testDecodeRejectsCorruptGzipBlob() {
    final LargeString bad =
        new LargeString().setCompression(CompressionType.GZIP).setBlob("not-base64-gzip!!");
    assertThrows(IllegalArgumentException.class, () -> LargeStrings.decode(bad));
  }
}
