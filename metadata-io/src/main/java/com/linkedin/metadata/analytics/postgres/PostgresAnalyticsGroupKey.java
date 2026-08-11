package com.linkedin.metadata.analytics.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nonnull;

/** Canonical group_key for analytics_rollup (stable hash of sorted dim map). */
public final class PostgresAnalyticsGroupKey {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private PostgresAnalyticsGroupKey() {}

  @Nonnull
  public static String of(@Nonnull Map<String, String> dims) {
    if (dims.isEmpty()) {
      return "";
    }
    TreeMap<String, String> sorted = new TreeMap<>();
    dims.forEach(
        (k, v) -> {
          if (k != null && v != null && !k.isBlank() && !v.isBlank()) {
            sorted.put(k, v);
          }
        });
    if (sorted.isEmpty()) {
      return "";
    }
    try {
      String json = MAPPER.writeValueAsString(sorted);
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(json.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(digest).substring(0, 32);
    } catch (JsonProcessingException | NoSuchAlgorithmException e) {
      throw new IllegalStateException("Failed to compute analytics group_key", e);
    }
  }

  @Nonnull
  public static Map<String, String> canonicalize(@Nonnull Map<String, String> dims) {
    TreeMap<String, String> sorted = new TreeMap<>();
    dims.forEach(
        (k, v) -> {
          if (k != null && v != null && !k.isBlank() && !v.isBlank()) {
            sorted.put(k, v);
          }
        });
    return new LinkedHashMap<>(sorted);
  }
}
