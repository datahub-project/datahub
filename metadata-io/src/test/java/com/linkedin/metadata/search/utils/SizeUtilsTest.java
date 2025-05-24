package com.linkedin.metadata.search.utils;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Unit tests for the SizeUtils class. */
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

  @ParameterizedTest
  @CsvSource({
    "1gb, 1024mb, true",
    "1024kb, 1mb, true",
    "1tb, 1024gb, true",
    "1pb, 1024tb, true",
    "1.5gb, 1536mb, true",
    "1536mb, 1.5gb, true",
    "1 GB, 1024 MB, true",
    "1024 MB, 1 GB, true"
  })
  void testIsEqualSize(String size1, String size2, boolean expected) {
    assertEquals(expected, SizeUtils.isEqualSize(size1, size2));
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

  @ParameterizedTest
  @CsvSource({"4%, 4", "3.5%, 3.5", "0.1%, 0.1", "0.01%, 0.01", "10.5%, 10.5", "4 %, 4"})
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
  }

  @ParameterizedTest
  @CsvSource({
    "1b, 1",
    "1kb, 1024",
    "1mb, 1048576",
    "1gb, 1073741824",
    "1tb, 1099511627776",
    "1pb, 1125899906842624",
    "1.5kb, 1536",
    "2.5gb, 2684354560",
    "1 mb, 1048576",
    "1 MB, 1048576",
    "1 MiB, 1048576"
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
  }

  @Test
  void testParseToBytes_UnknownUnit() {
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.parseToBytes("1 xyz"));
  }

  @ParameterizedTest
  @CsvSource({
    "1, 1 B",
    "1023, 1023 B",
    "1024, 1 KB",
    "1536, 1.5 KB",
    "1048576, 1 MB",
    "1073741824, 1 GB",
    "1099511627776, 1 TB",
    "1125899906842624, 1 PB",
    "2684354560, 2.5 GB"
  })
  void testFormatBytes(long bytes, String expected) {
    assertEquals(expected, SizeUtils.formatBytes(bytes));
  }

  @Test
  void testFormatBytes_NegativeValue() {
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.formatBytes(-1));
  }

  @ParameterizedTest
  @CsvSource({"4, 4%", "3.5, 3.5%", "0.1, 0.1%", "0.01, 0.01%", "10.5, 10.5%", "100, 100%"})
  void testFormatPercent(double value, String expected) {
    assertEquals(expected, SizeUtils.formatPercent(value));
  }

  @Test
  void testFormatPercent_NegativeValue() {
    assertThrows(IllegalArgumentException.class, () -> SizeUtils.formatPercent(-1));
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
}
