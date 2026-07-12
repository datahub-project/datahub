package com.linkedin.metadata.search.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.data.template.DoubleMap;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.search.features.Features;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Utilities to construct common {@code SearchEntity} payloads (features, extraFields) in a
 * client-agnostic way.
 *
 * <p>This mirrors logic currently embedded in the keyword path: - {@code
 * com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler#extractFeatures} -
 * {@code
 * com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler#getStringMap}
 *
 * <p>The helpers below accept primitive inputs (backend score and a source map), so they can be
 * used both from the HLRC-based keyword path and low-level REST based semantic path without pulling
 * in client-specific types.
 */
@Slf4j
public final class SearchResultUtils {

  private SearchResultUtils() {}

  /**
   * Build a {@link DoubleMap} of features consistent with the keyword path.
   *
   * <p>Always includes {@code SEARCH_BACKEND_SCORE}. Optionally includes {@code QUERY_COUNT} when
   * {@code usageCountLast30Days} is present in the source map.
   *
   * <p>Parity note: This is the low-level-client equivalent of {@code
   * com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler#extractFeatures}
   * which operates on HLRC {@code SearchHit}. Use this utility when you have only the backend score
   * and a {@code Map}-based source.
   */
  @Nonnull
  public static DoubleMap buildBaseFeatures(
      double backendScore, @Nullable Map<String, Object> sourceAsMap) {
    DoubleMap features = new DoubleMap();
    features.put(Features.Name.SEARCH_BACKEND_SCORE.toString(), backendScore);
    return features;
  }

  /**
   * Convert the provided source map into a {@link StringMap} by JSON-stringifying each value.
   *
   * <p>Matches {@code getStringMap(...)} behavior in the keyword path; failures to serialize a
   * particular field are ignored and the field is skipped.
   *
   * <p>Parity note: This is the low-level equivalent to {@code
   * com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler#getStringMap}.
   */
  @Nonnull
  public static StringMap toExtraFields(
      @Nonnull ObjectMapper objectMapper, @Nullable Map<String, Object> sourceAsMap) {
    StringMap stringMap = new StringMap();
    if (sourceAsMap == null) {
      return stringMap;
    }
    for (Map.Entry<String, Object> entry : sourceAsMap.entrySet()) {
      try {
        stringMap.put(entry.getKey(), objectMapper.writeValueAsString(entry.getValue()));
      } catch (IOException e) {
        // Log and skip fields that fail to serialize to maintain parity and aid debugging.
        log.warn("Failed to serialize extra field {}", entry.getKey(), e);
      }
    }
    return stringMap;
  }
}
