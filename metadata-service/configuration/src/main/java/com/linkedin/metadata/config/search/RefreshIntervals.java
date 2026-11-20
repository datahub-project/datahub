package com.linkedin.metadata.config.search;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Per-service Elasticsearch {@code refresh_interval} values, plus optional entity and timeseries
 * aspect overrides.
 *
 * <p>Seconds fields have no Java default. Product defaults live in {@code application.yaml}. {@link
 * #entities} and {@link #aspects} are JSON objects ({@code {"dataset": 10}}) because a Spring map
 * does not bind a JSON environment variable. Null means no overrides.
 */
@Slf4j
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class RefreshIntervals {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> RAW_MAP = new TypeReference<>() {};

  private Integer entitySeconds;
  private Integer graphSeconds;
  private Integer systemMetadataSeconds;
  private Integer timeseriesSeconds;
  private Integer usageSeconds;

  /** JSON object of entity name to seconds. Null when unset. */
  private String entities;

  /** JSON object of timeseries aspect name to seconds. Null when unset. */
  private String aspects;

  /**
   * Same interval for every service. The caller supplies the seconds; this is not a product
   * default.
   */
  public static RefreshIntervals allServices(int seconds) {
    return RefreshIntervals.builder()
        .entitySeconds(seconds)
        .graphSeconds(seconds)
        .systemMetadataSeconds(seconds)
        .timeseriesSeconds(seconds)
        .usageSeconds(seconds)
        .build();
  }

  public Map<String, Integer> entityOverrides() {
    return parseSecondsMap(entities);
  }

  public Map<String, Integer> aspectOverrides() {
    return parseSecondsMap(aspects);
  }

  /**
   * Sparse cluster overlay. A non-null service field replaces the shared value. Override maps merge
   * per key; an overlay key replaces that key and other keys stay.
   */
  public RefreshIntervals mergeOverlay(RefreshIntervals overlay) {
    if (overlay == null) {
      return this;
    }
    RefreshIntervalsBuilder builder = this.toBuilder();
    if (overlay.entitySeconds != null) {
      builder.entitySeconds(overlay.entitySeconds);
    }
    if (overlay.graphSeconds != null) {
      builder.graphSeconds(overlay.graphSeconds);
    }
    if (overlay.systemMetadataSeconds != null) {
      builder.systemMetadataSeconds(overlay.systemMetadataSeconds);
    }
    if (overlay.timeseriesSeconds != null) {
      builder.timeseriesSeconds(overlay.timeseriesSeconds);
    }
    if (overlay.usageSeconds != null) {
      builder.usageSeconds(overlay.usageSeconds);
    }
    if (overlay.entities != null) {
      builder.entities(writeSecondsMap(mergeMaps(entityOverrides(), overlay.entityOverrides())));
    }
    if (overlay.aspects != null) {
      builder.aspects(writeSecondsMap(mergeMaps(aspectOverrides(), overlay.aspectOverrides())));
    }
    return builder.build();
  }

  static Map<String, Integer> parseSecondsMap(String json) {
    if (json == null || json.isBlank() || isUnsetOverride(json)) {
      return Map.of();
    }
    final Map<String, Object> raw;
    try {
      raw = MAPPER.readValue(json, RAW_MAP);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "refreshIntervals override is not a JSON object: " + json, e);
    }
    if (raw == null) {
      return Map.of();
    }
    Map<String, Integer> parsed = new LinkedHashMap<>();
    for (Map.Entry<String, Object> entry : raw.entrySet()) {
      parsed.put(
          entry.getKey().toLowerCase(Locale.ROOT),
          requireWholeSeconds(entry.getKey(), entry.getValue()));
    }
    return Map.copyOf(parsed);
  }

  private static boolean isUnsetOverride(String json) {
    String trimmed = json.trim();
    // Configuration-property binding does not evaluate the SpEL default #{null}.
    return "null".equals(trimmed) || "#{null}".equals(trimmed);
  }

  private static int requireWholeSeconds(String key, Object value) {
    if (!(value instanceof Number number) || number.doubleValue() != number.longValue()) {
      throw new IllegalArgumentException(
          "refreshIntervals override for '" + key + "' must be a whole number of seconds");
    }
    long seconds = number.longValue();
    if (seconds > Integer.MAX_VALUE || seconds < Integer.MIN_VALUE) {
      throw new IllegalArgumentException(
          "refreshIntervals override for '" + key + "' is outside the supported range");
    }
    return (int) seconds;
  }

  private static Map<String, Integer> mergeMaps(
      Map<String, Integer> base, Map<String, Integer> overlay) {
    Map<String, Integer> merged = new LinkedHashMap<>(base);
    merged.putAll(overlay);
    return merged;
  }

  private static String writeSecondsMap(Map<String, Integer> seconds) {
    try {
      return MAPPER.writeValueAsString(seconds);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to write refreshIntervals override", e);
    }
  }
}
