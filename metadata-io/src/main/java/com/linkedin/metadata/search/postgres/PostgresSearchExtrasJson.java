package com.linkedin.metadata.search.postgres;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Builds {@code search_extras} JSONB for pgSearch: Elasticsearch {@code _search} shaped content
 * excluding {@code tier_*} consolidated text channels (those map to {@code search_text_tierN} /
 * {@code search_vector_tierN} / {@code embedding_tierN}).
 */
@Slf4j
public final class PostgresSearchExtrasJson {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private PostgresSearchExtrasJson() {}

  /**
   * @return JSON string for binding to {@code search_extras}, or {@code null} when absent or empty
   *     after stripping tier keys
   */
  @Nullable
  public static String buildSearchExtrasPayload(@Nullable String documentJson) {
    if (documentJson == null || documentJson.isBlank()) {
      return null;
    }
    try {
      JsonNode root = MAPPER.readTree(documentJson.getBytes(StandardCharsets.UTF_8));
      JsonNode search = root.get("_search");
      if (search == null || !search.isObject()) {
        return null;
      }
      ObjectNode copy = (ObjectNode) search.deepCopy();
      List<String> tierKeys = new ArrayList<>();
      copy.fieldNames().forEachRemaining(k -> tierKeys.add(k));
      for (String k : tierKeys) {
        if (k.startsWith("tier_")) {
          copy.remove(k);
        }
      }
      if (copy.isEmpty()) {
        return null;
      }
      return MAPPER.writeValueAsString(copy);
    } catch (Exception e) {
      log.debug("Could not derive search_extras from document: {}", e.toString());
      return null;
    }
  }
}
