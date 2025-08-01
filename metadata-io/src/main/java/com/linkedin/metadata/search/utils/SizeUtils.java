package com.linkedin.metadata.search.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for comparing size strings and percentage values. Supports size units: b, kb, mb,
 * gb, tb, pb (case insensitive). Supports percentage values like "4%", "0.1%", etc.
 */
public class SizeUtils {

  /**
   * Compares two size strings and determines if the first is larger than the second.
   *
   * @param size1 First size string (e.g., "1024mb")
   * @param size2 Second size string (e.g., "2gb")
   * @return true if size1 is greater than size2, false otherwise
   * @throws IllegalArgumentException if the input strings have invalid format
   */
  public static boolean isGreaterSize(String size1, String size2) {
    // Convert both sizes to bytes for comparison
    long bytes1 = parseToBytes(size1);
    long bytes2 = parseToBytes(size2);

    // Compare the byte values
    return bytes1 > bytes2;
  }

  /**
   * Compares two size strings and determines if they are equal.
   *
   * @param size1 First size string (e.g., "1024mb")
   * @param size2 Second size string (e.g., "1gb")
   * @return true if size1 equals size2, false otherwise
   * @throws IllegalArgumentException if the input strings have invalid format
   */
  public static boolean isEqualSize(String size1, String size2) {
    long bytes1 = parseToBytes(size1);
    long bytes2 = parseToBytes(size2);
    return bytes1 == bytes2;
  }

  /**
   * Compares two percentage values and determines if the first is larger than the second.
   *
   * @param percent1 First percentage value (e.g., "4%", "0.1%")
   * @param percent2 Second percentage value (e.g., "3%", "0.05%")
   * @return true if percent1 is greater than percent2, false otherwise
   * @throws IllegalArgumentException if the input strings have invalid format
   */
  public static boolean isGreaterPercent(String percent1, String percent2) {
    double value1 = parsePercentValue(percent1);
    double value2 = parsePercentValue(percent2);
    return value1 > value2;
  }

  /**
   * Parses a percentage string (e.g., "4%", "0.1%") to its numeric value.
   *
   * @param percentStr The percentage string to parse
   * @return The numeric value (e.g., 4.0 for "4%")
   * @throws IllegalArgumentException if the input string has invalid format
   */
  public static double parsePercentValue(String percentStr) {
    if (percentStr == null || percentStr.isEmpty()) {
      throw new IllegalArgumentException("Percentage string cannot be null or empty");
    }

    // Define the regex pattern to match a number followed by % symbol
    // This pattern matches: digits (potentially with decimal) followed by optional whitespace and %
    // symbol
    Pattern pattern = Pattern.compile("([\\d.]+)\\s*%");
    Matcher matcher = pattern.matcher(percentStr.trim());

    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid percentage format: " + percentStr);
    }

    // Extract the numeric value
    String valueStr = matcher.group(1);
    try {
      return Double.parseDouble(valueStr);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid numeric value in percentage: " + percentStr, e);
    }
  }

  /**
   * Parses a size string (e.g., "1024mb") to bytes.
   *
   * @param sizeStr The size string to parse
   * @return The size in bytes
   * @throws IllegalArgumentException if the input string has invalid format
   */
  public static long parseToBytes(String sizeStr) {
    if (sizeStr == null || sizeStr.isEmpty()) {
      throw new IllegalArgumentException("Size string cannot be null or empty");
    }

    // Define the regex pattern to match number and unit
    // This pattern matches: digits (potentially with decimal) followed by optional whitespace and a
    // unit
    Pattern pattern = Pattern.compile("([\\d.]+)\\s*([a-zA-Z]+)");
    Matcher matcher = pattern.matcher(sizeStr.trim());

    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid size format: " + sizeStr);
    }

    // Extract the numeric value and unit
    double value = Double.parseDouble(matcher.group(1));
    String unit = matcher.group(2).toLowerCase();

    // Define the multipliers for each unit (relative to bytes)
    Map<String, Long> unitMultipliers = new HashMap<>();
    unitMultipliers.put("b", 1L);
    unitMultipliers.put("byte", 1L);
    unitMultipliers.put("bytes", 1L);
    unitMultipliers.put("k", 1024L);
    unitMultipliers.put("kb", 1024L);
    unitMultipliers.put("kib", 1024L);
    unitMultipliers.put("kilobyte", 1024L);
    unitMultipliers.put("kilobytes", 1024L);
    unitMultipliers.put("m", 1024L * 1024L);
    unitMultipliers.put("mb", 1024L * 1024L);
    unitMultipliers.put("mib", 1024L * 1024L);
    unitMultipliers.put("megabyte", 1024L * 1024L);
    unitMultipliers.put("megabytes", 1024L * 1024L);
    unitMultipliers.put("g", 1024L * 1024L * 1024L);
    unitMultipliers.put("gb", 1024L * 1024L * 1024L);
    unitMultipliers.put("gib", 1024L * 1024L * 1024L);
    unitMultipliers.put("gigabyte", 1024L * 1024L * 1024L);
    unitMultipliers.put("gigabytes", 1024L * 1024L * 1024L);
    unitMultipliers.put("t", 1024L * 1024L * 1024L * 1024L);
    unitMultipliers.put("tb", 1024L * 1024L * 1024L * 1024L);
    unitMultipliers.put("tib", 1024L * 1024L * 1024L * 1024L);
    unitMultipliers.put("terabyte", 1024L * 1024L * 1024L * 1024L);
    unitMultipliers.put("terabytes", 1024L * 1024L * 1024L * 1024L);
    unitMultipliers.put("p", 1024L * 1024L * 1024L * 1024L * 1024L);
    unitMultipliers.put("pb", 1024L * 1024L * 1024L * 1024L * 1024L);
    unitMultipliers.put("pib", 1024L * 1024L * 1024L * 1024L * 1024L);
    unitMultipliers.put("petabyte", 1024L * 1024L * 1024L * 1024L * 1024L);
    unitMultipliers.put("petabytes", 1024L * 1024L * 1024L * 1024L * 1024L);

    // Get the multiplier for the unit
    Long multiplier = unitMultipliers.get(unit);
    if (multiplier == null) {
      throw new IllegalArgumentException("Unknown unit: " + unit);
    }

    // Calculate and return the value in bytes
    return (long) (value * multiplier);
  }

  /**
   * Formats a byte value to a human-readable size string.
   *
   * @param bytes The size in bytes
   * @return A human-readable size string (e.g., "2.5 GB")
   */
  public static String formatBytes(long bytes) {
    if (bytes < 0) {
      throw new IllegalArgumentException("Bytes cannot be negative");
    }

    final String[] units = {"B", "KB", "MB", "GB", "TB", "PB"};
    int unitIndex = 0;
    double size = bytes;

    while (size >= 1024 && unitIndex < units.length - 1) {
      size /= 1024;
      unitIndex++;
    }

    // Format with up to 2 decimal places, but remove trailing zeros
    String formatted = String.format("%.2f", size).replaceAll("\\.?0*$", "");

    return formatted + " " + units[unitIndex];
  }

  /**
   * Formats a numeric value as a percentage string.
   *
   * @param value The numeric value (e.g., 4.0)
   * @return A percentage string (e.g., "4.0%")
   */
  public static String formatPercent(double value) {
    if (value < 0) {
      throw new IllegalArgumentException("Percentage cannot be negative");
    }

    // Format with up to 2 decimal places, but remove trailing zeros
    String formatted = String.format("%.2f", value).replaceAll("\\.?0*$", "");

    return formatted + "%";
  }
}
