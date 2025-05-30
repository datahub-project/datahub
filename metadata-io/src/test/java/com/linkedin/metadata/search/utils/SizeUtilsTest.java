package com.linkedin.metadata.search.utils;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Unit tests for the SizeUtils class with enhanced coverage. */
public class SizeUtilsTest {

  @ParameterizedTest
  @CsvSource({
    "1024mb, 2gb, false",
    "3gb, 2048mb, true",
    "1tb, 1000gb, true",
    "1024kb, 1mb, false",
    "1.5gb, 1500mb, false",
    "1.5gb, 1536mb, false",
    "1537mb, 1.5gb, true",
    "1pb, 1024tb, false",
    "1025tb, 1pb, true",
    "1 gb, 1000 mb, true",
    "1 GB, 1000 MB, true",
    "1 GiB, 1024 MiB, false",
    "1025 MiB, 1 GiB, true"
  })
  void testIsGreaterSize(String size1, String size2, boolean expected) {
    assertEquals(expected, SizeUtils.isGreaterSize(size1, size2));
  }

  @Test
  void testIsGreaterSize_EdgeCases() {
    // Test with identical sizes
    assertFalse(SizeUtils.isGreaterSize("1gb", "1gb"));
    assertFalse(SizeUtils.isGreaterSize("1024mb", "1gb"));

    // Test with very small sizes
    assertTrue(SizeUtils.isGreaterSize("2b", "1b"));
    assertFalse(SizeUtils.isGreaterSize("1b", "2b"));

    // Test with decimal values
    assertTrue(SizeUtils.isGreaterSize("1.1gb", "1gb"));
    assertFalse(SizeUtils.isGreaterSize("1gb", "1.1gb"));

    // Test with mixed case units
    assertTrue(SizeUtils.isGreaterSize("2GB", "1gb"));
    assertTrue(SizeUtils.isGreaterSize("2gb", "1GB"));
  }

  @Test
  void testIsGreaterSize_InvalidInputs() {
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.isGreaterSize(null, "1gb"));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.isGreaterSize("1gb", null));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.isGreaterSize("", "1gb"));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.isGreaterSize("1gb", ""));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.isGreaterSize("invalid", "1gb"));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.isGreaterSize("1gb", "invalid"));
  }

  @ParameterizedTest
  @CsvSource({
    "1gb, 1024mb, true",
    "1024kb, 1mb, true",
    "1tb, 1024gb, true",
    "1pb, 1024tb, true",
    "1.5gb, 1536mb, true",
    "1536mb, 1.5gb, true",
    "1 GB, 1024 MB, true",
    "1024 MB, 1 GB, true",
    "1b, 1byte, true",
    "1kb, 1k, true",
    "1mb, 1m, true",
    "1gb, 1g, true",
    "1tb, 1t, true",
    "1pb, 1p, true"
  })
  void testIsEqualSize(String size1, String size2, boolean expected) {
    assertEquals(expected, SizeUtils.isEqualSize(size1, size2));
  }

  @Test
  void testIsEqualSize_EdgeCases() {
    // Test with same string
    assertTrue(SizeUtils.isEqualSize("1gb", "1gb"));

    // Test with different but equivalent representations
    assertTrue(SizeUtils.isEqualSize("1024bytes", "1kb"));
    assertTrue(SizeUtils.isEqualSize("1kilobyte", "1kb"));
    assertTrue(SizeUtils.isEqualSize("1megabyte", "1mb"));
    assertTrue(SizeUtils.isEqualSize("1gigabyte", "1gb"));
    assertTrue(SizeUtils.isEqualSize("1terabyte", "1tb"));
    assertTrue(SizeUtils.isEqualSize("1petabyte", "1pb"));

    // Test with decimal equivalents
    assertTrue(SizeUtils.isEqualSize("0.5gb", "512mb"));

    // Test non-equal sizes
    assertFalse(SizeUtils.isEqualSize("1gb", "1mb"));
    assertFalse(SizeUtils.isEqualSize("1.1gb", "1gb"));
  }

  @Test
  void testIsEqualSize_InvalidInputs() {
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.isEqualSize(null, "1gb"));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.isEqualSize("1gb", null));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.isEqualSize("", "1gb"));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.isEqualSize("1gb", ""));
  }

  @ParameterizedTest
  @CsvSource({
    "4%, 3%, true",
    "4%, 4%, false",
    "3%, 4%, false",
    "0.1%, 0.05%, true",
    "10.5%, 10%, true",
    "10%, 10.5%, false",
    "0.01%, 0.1%, false",
    "4 %, 3 %, true",
    "4 %, 3%, true",
    "4%, 3 %, true"
  })
  void testIsGreaterPercent(String percent1, String percent2, boolean expected) {
    assertEquals(expected, SizeUtils.isGreaterPercent(percent1, percent2));
  }

  @Test
  void testIsGreaterPercent_EdgeCases() {
    // Test with identical percentages
    assertFalse(SizeUtils.isGreaterPercent("5%", "5%"));
    assertFalse(SizeUtils.isGreaterPercent("5.0%", "5%"));

    // Test with very small differences
    assertTrue(SizeUtils.isGreaterPercent("5.01%", "5%"));
    assertFalse(SizeUtils.isGreaterPercent("5%", "5.01%"));

    // Test with zero
    assertTrue(SizeUtils.isGreaterPercent("0.1%", "0%"));
    assertFalse(SizeUtils.isGreaterPercent("0%", "0.1%"));

    // Test with large percentages
    assertTrue(SizeUtils.isGreaterPercent("100%", "99.9%"));
    assertTrue(SizeUtils.isGreaterPercent("150%", "100%"));
  }

  @Test
  void testIsGreaterPercent_InvalidInputs() {
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.isGreaterPercent(null, "5%"));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.isGreaterPercent("5%", null));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.isGreaterPercent("", "5%"));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.isGreaterPercent("5%", ""));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.isGreaterPercent("invalid", "5%"));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.isGreaterPercent("5%", "invalid"));
  }

  @ParameterizedTest
  @CsvSource({
    "4%, 4",
    "3.5%, 3.5",
    "0.1%, 0.1",
    "0.01%, 0.01",
    "10.5%, 10.5",
    "4 %, 4",
    "0%, 0",
    "100%, 100",
    "0.0001%, 0.0001"
  })
  void testParsePercentValue(String percentStr, double expectedValue) {
    assertEquals(expectedValue, SizeUtils.parsePercentValue(percentStr), 0.0001);
  }

  @Test
  void testParsePercentValue_InvalidFormat() {
    assertThrows(
        IllegalArgumentException.class, () -> SizeUtils.parsePercentValue("not a percentage"));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.parsePercentValue("4"));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.parsePercentValue("4%%"));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.parsePercentValue(""));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.parsePercentValue(null));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.parsePercentValue("%"));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.parsePercentValue("abc%"));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.parsePercentValue("4.5.5%"));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.parsePercentValue("% 4"));
  }

  @Test
  void testParsePercentValue_WithWhitespace() {
    assertEquals(4.0, SizeUtils.parsePercentValue("  4%  "), 0.0001);
    assertEquals(5.5, SizeUtils.parsePercentValue("\t5.5%\n"), 0.0001);
    assertEquals(0.1, SizeUtils.parsePercentValue(" 0.1 % "), 0.0001);
  }

  @ParameterizedTest
  @CsvSource({
    "1b, 1",
    "1byte, 1",
    "1bytes, 1",
    "1k, 1024",
    "1kb, 1024",
    "1kib, 1024",
    "1kilobyte, 1024",
    "1kilobytes, 1024",
    "1m, 1048576",
    "1mb, 1048576",
    "1mib, 1048576",
    "1megabyte, 1048576",
    "1megabytes, 1048576",
    "1g, 1073741824",
    "1gb, 1073741824",
    "1gib, 1073741824",
    "1gigabyte, 1073741824",
    "1gigabytes, 1073741824",
    "1t, 1099511627776",
    "1tb, 1099511627776",
    "1tib, 1099511627776",
    "1terabyte, 1099511627776",
    "1terabytes, 1099511627776",
    "1p, 1125899906842624",
    "1pb, 1125899906842624",
    "1pib, 1125899906842624",
    "1petabyte, 1125899906842624",
    "1petabytes, 1125899906842624",
    "1.5kb, 1536",
    "2.5gb, 2684354560",
    "1 mb, 1048576",
    "1 MB, 1048576",
    "1 MiB, 1048576",
    "0.5gb, 536870912",
    "0.25tb, 274877906944"
  })
  void testParseToBytes(String sizeStr, long expectedBytes) {
    assertEquals(expectedBytes, SizeUtils.parseToBytes(sizeStr));
  }

  @Test
  void testParseToBytes_InvalidFormat() {
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.parseToBytes("not a size"));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.parseToBytes("123"));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.parseToBytes(""));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.parseToBytes(null));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.parseToBytes("gb"));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.parseToBytes("1.2.3gb"));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.parseToBytes("abcgb"));
  }

  @Test
  void testParseToBytes_UnknownUnit() {
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.parseToBytes("1 xyz"));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.parseToBytes("1 unknown"));
    assertThrows(
        IllegalArgumentException.class,
        () -> SizeUtils.parseToBytes("1 zb")); // zettabyte not supported
    assertThrows(
        IllegalArgumentException.class,
        () -> SizeUtils.parseToBytes("1 yb")); // yottabyte not supported
  }

  @Test
  void testParseToBytes_WithWhitespace() {
    assertEquals(1048576L, SizeUtils.parseToBytes("  1mb  "));
    assertEquals(1073741824L, SizeUtils.parseToBytes("\t1gb\n"));
    assertEquals(1024L, SizeUtils.parseToBytes(" 1 kb "));
    assertEquals(2048L, SizeUtils.parseToBytes("2    kb"));
  }

  @Test
  void testParseToBytes_NumberFormatException() {
    // This should trigger NumberFormatException within parseToBytes
    // which should be wrapped in IllegalArgumentException
    String invalidInput = "1.2.3.4mb";
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> SizeUtils.parseToBytes(invalidInput));
    // The exception should contain information about the original NumberFormatException
    assertTrue(exception.getMessage().contains("Invalid size format"));
  }

  @ParameterizedTest
  @CsvSource({
    "0, 0 B",
    "1, 1 B",
    "1023, 1023 B",
    "1024, 1 KB",
    "1536, 1.5 KB",
    "1048576, 1 MB",
    "1572864, 1.5 MB",
    "1073741824, 1 GB",
    "1610612736, 1.5 GB",
    "1099511627776, 1 TB",
    "1649267441664, 1.5 TB",
    "1125899906842624, 1 PB",
    "1688849860263936, 1.5 PB",
    "2684354560, 2.5 GB",
    "5368709120, 5 GB",
    "10737418240, 10 GB"
  })
  void testFormatBytes(long bytes, String expected) {
    assertEquals(expected, SizeUtils.formatBytes(bytes));
  }

  @Test
  void testFormatBytes_NegativeValue() {
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.formatBytes(-1));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.formatBytes(-1000));
  }

  @Test
  void testFormatBytes_LargeValues() {
    // Test very large values that approach Long.MAX_VALUE
    String result = SizeUtils.formatBytes(Long.MAX_VALUE);
    assertNotNull(result);
    assertTrue(
        result.contains("EB") || result.contains("PB")); // Should be in exabytes or petabytes range
  }

  @Test
  void testFormatBytes_EdgeCases() {
    // Test boundary values
    assertEquals("1023 B", SizeUtils.formatBytes(1023));
    assertEquals("1 KB", SizeUtils.formatBytes(1024));
    assertEquals("1023 KB", SizeUtils.formatBytes(1024 * 1023));
    assertEquals("1 MB", SizeUtils.formatBytes(1024 * 1024));

    // Test decimal formatting
    assertEquals("1.5 KB", SizeUtils.formatBytes(1536));
    assertEquals("2.25 KB", SizeUtils.formatBytes(2304));

    // Test whole numbers don't show decimals
    assertEquals("2 KB", SizeUtils.formatBytes(2048));
    assertEquals("3 MB", SizeUtils.formatBytes(3145728));
  }

  @ParameterizedTest
  @CsvSource({
    "0, 0%",
    "4, 4%",
    "3.5, 3.5%",
    "0.1, 0.1%",
    "0.01, 0.01%",
    "10.5, 10.5%",
    "100, 100%",
    "0.001, 0.001%",
    "999.99, 999.99%",
    "1000, 1000%"
  })
  void testFormatPercent(double value, String expected) {
    assertEquals(expected, SizeUtils.formatPercent(value));
  }

  @Test
  void testFormatPercent_NegativeValue() {
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.formatPercent(-1));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.formatPercent(-0.1));
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.formatPercent(-100));
  }

  @Test
  void testFormatPercent_EdgeCases() {
    // Test whole numbers don't show decimals
    assertEquals("5%", SizeUtils.formatPercent(5.0));
    assertEquals("10%", SizeUtils.formatPercent(10.00));

    // Test decimal formatting
    assertEquals("5.5%", SizeUtils.formatPercent(5.5));
    assertEquals("0.25%", SizeUtils.formatPercent(0.25));

    // Test very small values
    assertEquals("0.01%", SizeUtils.formatPercent(0.01));
    assertEquals("0.001%", SizeUtils.formatPercent(0.001));

    // Test large values
    assertEquals("1000%", SizeUtils.formatPercent(1000.0));
    assertEquals("9999.99%", SizeUtils.formatPercent(9999.99));
  }

  @Test
  void testEndToEnd_Size() {
    // Parse string to bytes and back to string
    String original = "2.5gb";
    long bytes = SizeUtils.parseToBytes(original);
    String formatted = SizeUtils.formatBytes(bytes);
    assertEquals("2.5 GB", formatted);

    // Compare sizes correctly
    assertTrue(SizeUtils.isGreaterSize("2.5gb", "2gb"));
    assertTrue(SizeUtils.isGreaterSize("2.5gb", "2400mb"));
    assertFalse(SizeUtils.isGreaterSize("2.5gb", "2600mb"));
    assertTrue(SizeUtils.isEqualSize("2.5gb", "2560mb"));
  }

  @Test
  void testEndToEnd_Percent() {
    // Parse string to value and back to string
    String original = "4.5%";
    double value = SizeUtils.parsePercentValue(original);
    String formatted = SizeUtils.formatPercent(value);
    assertEquals("4.5%", formatted);

    // Compare percentages correctly
    assertTrue(SizeUtils.isGreaterPercent("4.5%", "4%"));
    assertTrue(SizeUtils.isGreaterPercent("4.5%", "4.49%"));
    assertFalse(SizeUtils.isGreaterPercent("4.5%", "4.51%"));
    assertFalse(SizeUtils.isGreaterPercent("4.5%", "5%"));
  }

  @Test
  void testEndToEnd_ComplexScenarios() {
    // Test round-trip conversions
    String[] testSizes = {"1b", "512kb", "1.5mb", "2.25gb", "0.75tb", "1.125pb"};

    for (String size : testSizes) {
      long bytes = SizeUtils.parseToBytes(size);
      String formatted = SizeUtils.formatBytes(bytes);
      long reparsedBytes = SizeUtils.parseToBytes(formatted.toLowerCase().replace(" ", ""));

      // Should be equal or very close due to formatting precision
      assertEquals(
          bytes,
          reparsedBytes,
          bytes * 0.01, // Allow 1% tolerance for rounding
          "Round-trip conversion failed for: " + size);
    }
  }

  @Test
  void testComparisonConsistency() {
    String[] sizes = {"100b", "1kb", "1mb", "1gb", "1tb"};

    // Test that comparison methods are consistent
    for (int i = 0; i < sizes.length; i++) {
      for (int j = 0; j < sizes.length; j++) {
        String size1 = sizes[i];
        String size2 = sizes[j];

        boolean isGreater = SizeUtils.isGreaterSize(size1, size2);
        boolean isEqual = SizeUtils.isEqualSize(size1, size2);
        boolean isLess = SizeUtils.isGreaterSize(size2, size1);

        // Exactly one of these should be true
        int trueCount = (isGreater ? 1 : 0) + (isEqual ? 1 : 0) + (isLess ? 1 : 0);
        assertEquals(
            1,
            trueCount,
            String.format(
                "Inconsistent comparison for %s vs %s: greater=%s, equal=%s, less=%s",
                size1, size2, isGreater, isEqual, isLess));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"1.0kb", "2.00mb", "3.000gb", "4.0000tb", "5.00000pb"})
  void testParseToBytes_DecimalPrecision(String sizeStr) {
    // These should all parse successfully despite trailing zeros
    long bytes = SizeUtils.parseToBytes(sizeStr);
    assertTrue(bytes > 0, "Should parse decimal with trailing zeros: " + sizeStr);
  }

  @Test
  void testFormatBytes_TrailingZeroRemoval() {
    // Test that trailing zeros are properly removed in formatting
    assertEquals("1 KB", SizeUtils.formatBytes(1024)); // Should not show .00
    assertEquals("1.5 KB", SizeUtils.formatBytes(1536)); // Should show .5
    assertEquals("1.25 KB", SizeUtils.formatBytes(1280)); // Should show .25
    assertEquals("1.1 KB", SizeUtils.formatBytes(1126)); // Should show .1 (approximately)
  }

  @Test
  void testFormatPercent_TrailingZeroRemoval() {
    // Test that trailing zeros are properly removed in formatting
    assertEquals("5%", SizeUtils.formatPercent(5.00)); // Should not show .00
    assertEquals("5.5%", SizeUtils.formatPercent(5.50)); // Should show .5
    assertEquals("5.25%", SizeUtils.formatPercent(5.25)); // Should show .25
    assertEquals("5.1%", SizeUtils.formatPercent(5.10)); // Should show .1
  }

  @Test
  void testParsePercentValue_ExtremeValues() {
    // Test very small percentages
    assertEquals(0.0001, SizeUtils.parsePercentValue("0.0001%"), 0.000001);

    // Test very large percentages
    assertEquals(9999.99, SizeUtils.parsePercentValue("9999.99%"), 0.01);

    // Test integer percentages
    assertEquals(1000.0, SizeUtils.parsePercentValue("1000%"), 0.01);
  }

  @Test
  void testParseToBytes_ExtremeValues() {
    // Test very small sizes
    assertEquals(1L, SizeUtils.parseToBytes("0.001kb"));

    // Test fractional bytes (should round down)
    assertEquals(1L, SizeUtils.parseToBytes("1.9b"));

    // Test very large sizes (within petabyte range)
    assertTrue(SizeUtils.parseToBytes("999pb") > 0);
  }
}
