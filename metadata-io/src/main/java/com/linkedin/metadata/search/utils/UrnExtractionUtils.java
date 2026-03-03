package com.linkedin.metadata.search.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHit;

/**
 * Utility class for safely extracting URNs from Elasticsearch documents. Provides null checking and
 * detailed logging for debugging purposes.
 */
@Slf4j
public class UrnExtractionUtils {

  private UrnExtractionUtils() {
    // Utility class, prevent instantiation
  }

  /**
   * Safely extracts a URN from a search hit's source map.
   *
   * @param hit The search hit containing the document
   * @return The extracted URN
   * @throws RuntimeException if the URN field is null or invalid
   */
  @Nonnull
  public static Urn extractUrnFromSearchHit(@Nonnull SearchHit hit) {
    Map<String, Object> sourceMap = hit.getSourceAsMap();
    Object urnValue = sourceMap.get("urn");

    if (urnValue == null) {
      log.error(
          "Found search document with null URN. Document details: index={}, id={}, source={}",
          hit.getIndex(),
          hit.getId(),
          sourceMap);
      throw new RuntimeException(
          "Search document contains null URN. Index: " + hit.getIndex() + ", ID: " + hit.getId());
    }

    try {
      return Urn.createFromString(urnValue.toString());
    } catch (URISyntaxException e) {
      log.error(
          "Invalid URN in search document. Index: {}, ID: {}, URN value: {}, Full source: {}",
          hit.getIndex(),
          hit.getId(),
          urnValue,
          sourceMap,
          e);
      throw new RuntimeException("Invalid urn in search document " + e);
    }
  }

  /**
   * Safely extracts a URN from a nested map within a document, returning null instead of throwing.
   *
   * @param document The document containing the nested map
   * @param nestedFieldName The field name containing the nested map (e.g., "source", "destination")
   * @param context Additional context for logging (e.g., "source", "destination")
   * @return The extracted URN, or null if extraction failed
   */
  @Nullable
  public static Urn extractUrnFromNestedFieldSafely(
      @Nonnull Map<String, Object> document,
      @Nonnull String nestedFieldName,
      @Nonnull String context) {

    try {
      Map<String, Object> nestedMap = (Map<String, Object>) document.get(nestedFieldName);
      if (nestedMap == null) {
        log.error(
            "Found document with null {} field. Document details: {}", nestedFieldName, document);
        return null;
      }

      Object urnValue = nestedMap.get("urn");
      if (urnValue == null) {
        log.error("Found document with null {} URN. Document details: {}", context, document);
        return null;
      }

      return UrnUtils.getUrn(urnValue.toString());
    } catch (Exception e) {
      log.warn("Failed to extract {} URN from document: {}", context, e.getMessage());
      return null;
    }
  }

  /**
   * Extract unique URNs from a search response, skipping invalid entries.
   *
   * <p>This method iterates through all hits in the response and extracts URNs, collecting them
   * into a Set for deduplication. Invalid or null URNs are logged and skipped rather than causing
   * the entire operation to fail.
   *
   * @param response The search response containing hits
   * @return Set of unique URNs extracted from the response
   */
  @Nonnull
  public static Set<Urn> extractUniqueUrns(@Nonnull SearchResponse response) {
    Set<Urn> urns = new HashSet<>();
    if (response.getHits() == null || response.getHits().getHits() == null) {
      return urns;
    }
    for (SearchHit hit : response.getHits().getHits()) {
      try {
        urns.add(extractUrnFromSearchHit(hit));
      } catch (Exception e) {
        log.warn("Skipping hit with invalid URN: {}", e.getMessage());
      }
    }
    return urns;
  }
}
