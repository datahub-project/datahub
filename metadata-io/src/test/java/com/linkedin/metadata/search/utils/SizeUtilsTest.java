package com.linkedin.metadata.search.utils;

import static org.testng.Assert.*;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Unit tests for the SizeUtils class with enhanced coverage. */
public class SizeUtilsTest {

  @DataProvider(name = "isGreaterSizeData")
  public Object[][] provideIsGreaterSizeData() {
    return new Object[][] {
      {"1024mb", "2gb", false},
      {"3gb", "2048mb", true},
      {"1tb", "1000gb", true},
      {"1024kb", "1mb", false},
      {"1.5gb", "1500mb", true}, // 1.5GB = 1536MB > 1500MB
      {"1.5gb", "1536mb", false},
      {"1537mb", "1.5gb", true},
      {"1pb", "1024tb", false},
      {"1025tb", "1pb", true},
      {"1 gb", "1000 mb", true},
      {"1 GB", "1000 MB", true}, // 1 GB = 1024 MB, so 1 GB > 1000 MB
      {"1 GiB", "1024 MiB", false},
      {"1025 MiB", "1 GiB", true}
    };
  }

  @Test(dataProvider = "isGreaterSizeData")
  void testIsGreaterSize(String size1, String size2, boolean expected) {
    assertEquals(SizeUtils.isGreaterSize(size1, size2), expected);
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

  @DataProvider(name = "isEqualSizeData")
  public Object[][] provideIsEqualSizeData() {
    return new Object[][] {
      {"1gb", "1024mb", true},
      {"1024kb", "1mb", true},
      {"1tb", "1024gb", true},
      {"1pb", "1024tb", true},
      {"1.5gb", "1536mb", true},
      {"1536mb", "1.5gb", true},
      {"1 GB", "1024 MB", true},
      {"1024 MB", "1 GB", true},
      {"1b", "1byte", true},
      {"1kb", "1k", true},
      {"1mb", "1m", true},
      {"1gb", "1g", true},
      {"1tb", "1t", true},
      {"1pb", "1p", true}
    };
  }

  @Test(dataProvider = "isEqualSizeData")
  void testIsEqualSize(String size1, String size2, boolean expected) {
    assertEquals(SizeUtils.isEqualSize(size1, size2), expected);
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

  @DataProvider(name = "isGreaterPercentData")
  public Object[][] provideIsGreaterPercentData() {
    return new Object[][] {
      {"4%", "3%", true},
      {"4%", "4%", false},
      {"3%", "4%", false},
      {"0.1%", "0.05%", true},
      {"10.5%", "10%", true},
      {"10%", "10.5%", false},
      {"0.01%", "0.1%", false},
      {"4 %", "3 %", true},
      {"4 %", "3%", true},
      {"4%", "3 %", true}
    };
  }

  @Test(dataProvider = "isGreaterPercentData")
  void testIsGreaterPercent(String percent1, String percent2, boolean expected) {
    assertEquals(SizeUtils.isGreaterPercent(percent1, percent2), expected);
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

  @DataProvider(name = "parsePercentValueData")
  public Object[][] provideParsePercentValueData() {
    return new Object[][] {
      {"4%", 4.0},
      {"3.5%", 3.5},
      {"0.1%", 0.1},
      {"0.01%", 0.01},
      {"10.5%", 10.5},
      {"4 %", 4.0},
      {"0%", 0.0},
      {"100%", 100.0},
      {"0.0001%", 0.0001}
    };
  }

  @Test(dataProvider = "parsePercentValueData")
  void testParsePercentValue(String percentStr, double expectedValue) {
    assertEquals(SizeUtils.parsePercentValue(percentStr), expectedValue, 0.0001);
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
    assertEquals(SizeUtils.parsePercentValue("  4%  "), 4.0, 0.0001);
    assertEquals(SizeUtils.parsePercentValue("\t5.5%\n"), 5.5, 0.0001);
    assertEquals(SizeUtils.parsePercentValue(" 0.1 % "), 0.1, 0.0001);
  }

  @DataProvider(name = "parseToBytesData")
  public Object[][] provideParseToBytesData() {
    return new Object[][] {
      {"1b", 1L},
      {"1byte", 1L},
      {"1bytes", 1L},
      {"1k", 1024L},
      {"1kb", 1024L},
      {"1kib", 1024L},
      {"1kilobyte", 1024L},
      {"1kilobytes", 1024L},
      {"1m", 1048576L},
      {"1mb", 1048576L},
      {"1mib", 1048576L},
      {"1megabyte", 1048576L},
      {"1megabytes", 1048576L},
      {"1g", 1073741824L},
      {"1gb", 1073741824L},
      {"1gib", 1073741824L},
      {"1gigabyte", 1073741824L},
      {"1gigabytes", 1073741824L},
      {"1t", 1099511627776L},
      {"1tb", 1099511627776L},
      {"1tib", 1099511627776L},
      {"1terabyte", 1099511627776L},
      {"1terabytes", 1099511627776L},
      {"1p", 1125899906842624L},
      {"1pb", 1125899906842624L},
      {"1pib", 1125899906842624L},
      {"1petabyte", 1125899906842624L},
      {"1petabytes", 1125899906842624L},
      {"1.5kb", 1536L},
      {"2.5gb", 2684354560L},
      {"1 mb", 1048576L},
      {"1 MB", 1048576L},
      {"1 MiB", 1048576L},
      {"0.5gb", 536870912L},
      {"0.25tb", 274877906944L}
    };
  }

  @Test(dataProvider = "parseToBytesData")
  void testParseToBytes(String sizeStr, long expectedBytes) {
    assertEquals(SizeUtils.parseToBytes(sizeStr), expectedBytes);
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
    assertEquals(SizeUtils.parseToBytes("  1mb  "), 1048576L);
    assertEquals(SizeUtils.parseToBytes("\t1gb\n"), 1073741824L);
    assertEquals(SizeUtils.parseToBytes(" 1 kb "), 1024L);
    assertEquals(SizeUtils.parseToBytes("2    kb"), 2048L);
  }

  @Test
  void testParseToBytes_NumberFormatException() {
    // This should trigger NumberFormatException within parseToBytes
    // which should be wrapped in IllegalArgumentException
    String invalidInput = "1.2.3.4mb";
    try {
      SizeUtils.parseToBytes(invalidInput);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException exception) {
      // The exception message is "multiple points" - just verify we got the right exception type
      assertNotNull(exception.getMessage());
    }
  }

  @DataProvider(name = "formatBytesData")
  public Object[][] provideFormatBytesData() {
    return new Object[][] {
      {0L, "0 B"},
      {1L, "1 B"},
      {1023L, "1023 B"},
      {1024L, "1 KB"},
      {1536L, "1.5 KB"},
      {1048576L, "1 MB"},
      {1572864L, "1.5 MB"},
      {1073741824L, "1 GB"},
      {1610612736L, "1.5 GB"},
      {1099511627776L, "1 TB"},
      {1649267441664L, "1.5 TB"},
      {1125899906842624L, "1 PB"},
      {1688849860263936L, "1.5 PB"},
      {2684354560L, "2.5 GB"},
      {5368709120L, "5 GB"},
      {10737418240L, "10 GB"}
    };
  }

  @Test(dataProvider = "formatBytesData")
  void testFormatBytes(long bytes, String expected) {
    assertEquals(SizeUtils.formatBytes(bytes), expected);
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
    assertEquals(SizeUtils.formatBytes(1023), "1023 B");
    assertEquals(SizeUtils.formatBytes(1024), "1 KB");
    assertEquals(SizeUtils.formatBytes(1024 * 1023), "1023 KB");
    assertEquals(SizeUtils.formatBytes(1024 * 1024), "1 MB");

    // Test decimal formatting
    assertEquals(SizeUtils.formatBytes(1536), "1.5 KB");
    assertEquals(SizeUtils.formatBytes(2304), "2.25 KB");

    // Test whole numbers don't show decimals
    assertEquals(SizeUtils.formatBytes(2048), "2 KB");
    assertEquals(SizeUtils.formatBytes(3145728), "3 MB");
  }

  @DataProvider(name = "formatPercentData")
  public Object[][] provideFormatPercentData() {
    return new Object[][] {
      {0.0, "0%"},
      {4.0, "4%"},
      {3.5, "3.5%"},
      {0.1, "0.1%"},
      {0.01, "0.01%"},
      {10.5, "10.5%"},
      {100.0, "100%"},
      {0.001, "0%"},
      {999.99, "999.99%"},
      {1000.0, "1000%"}
    };
  }

  @Test(dataProvider = "formatPercentData")
  void testFormatPercent(double value, String expected) {
    assertEquals(SizeUtils.formatPercent(value), expected);
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
    assertEquals(SizeUtils.formatPercent(5.0), "5%");
    assertEquals(SizeUtils.formatPercent(10.00), "10%");

    // Test decimal formatting
    assertEquals(SizeUtils.formatPercent(5.5), "5.5%");
    assertEquals(SizeUtils.formatPercent(0.25), "0.25%");

    // Test very small values - these may be rounded to 0% due to formatting precision
    assertEquals(SizeUtils.formatPercent(0.01), "0.01%");
    assertEquals(SizeUtils.formatPercent(0.001), "0%"); // Very small values may round to 0%

    // Test large values
    assertEquals(SizeUtils.formatPercent(1000.0), "1000%");
    assertEquals(SizeUtils.formatPercent(9999.99), "9999.99%");
  }

  @Test
  void testEndToEnd_Size() {
    // Parse string to bytes and back to string
    String original = "2.5gb";
    long bytes = SizeUtils.parseToBytes(original);
    String formatted = SizeUtils.formatBytes(bytes);
    assertEquals(formatted, "2.5 GB");

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
    assertEquals(formatted, "4.5%");

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
          reparsedBytes,
          bytes,
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
            trueCount,
            1,
            String.format(
                "Inconsistent comparison for %s vs %s: greater=%s, equal=%s, less=%s",
                size1, size2, isGreater, isEqual, isLess));
      }
    }
  }

  @DataProvider(name = "decimalPrecisionData")
  public Object[][] provideDecimalPrecisionData() {
    return new Object[][] {{"1.0kb"}, {"2.00mb"}, {"3.000gb"}, {"4.0000tb"}, {"5.00000pb"}};
  }

  @Test(dataProvider = "decimalPrecisionData")
  void testParseToBytes_DecimalPrecision(String sizeStr) {
    // These should all parse successfully despite trailing zeros
    long bytes = SizeUtils.parseToBytes(sizeStr);
    assertTrue(bytes > 0, "Should parse decimal with trailing zeros: " + sizeStr);
  }

  @Test
  void testFormatBytes_TrailingZeroRemoval() {
    // Test that trailing zeros are properly removed in formatting
    assertEquals(SizeUtils.formatBytes(1024), "1 KB"); // Should not show .00
    assertEquals(SizeUtils.formatBytes(1536), "1.5 KB"); // Should show .5
    assertEquals(SizeUtils.formatBytes(1280), "1.25 KB"); // Should show .25
    assertEquals(SizeUtils.formatBytes(1126), "1.1 KB"); // Should show .1 (approximately)
  }

  @Test
  void testFormatPercent_TrailingZeroRemoval() {
    // Test that trailing zeros are properly removed in formatting
    assertEquals(SizeUtils.formatPercent(5.00), "5%"); // Should not show .00
    assertEquals(SizeUtils.formatPercent(5.50), "5.5%"); // Should show .5
    assertEquals(SizeUtils.formatPercent(5.25), "5.25%"); // Should show .25
    assertEquals(SizeUtils.formatPercent(5.10), "5.1%"); // Should show .1
  }

  @Test
  void testParsePercentValue_ExtremeValues() {
    // Test very small percentages
    assertEquals(SizeUtils.parsePercentValue("0.0001%"), 0.0001, 0.000001);

    // Test very large percentages
    assertEquals(SizeUtils.parsePercentValue("9999.99%"), 9999.99, 0.01);

    // Test integer percentages
    assertEquals(SizeUtils.parsePercentValue("1000%"), 1000.0, 0.01);
  }

  @Test
  void testParseToBytes_ExtremeValues() {
    // Test very small sizes
    assertEquals(SizeUtils.parseToBytes("0.001kb"), 1L);

    // Test fractional bytes (should round down)
    assertEquals(SizeUtils.parseToBytes("1.9b"), 1L);

    // Test very large sizes (within petabyte range)
    assertTrue(SizeUtils.parseToBytes("999pb") > 0);
  }
}
