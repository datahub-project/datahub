package com.linkedin.metadata.search.postgres;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Denormalized {@code entity_type} column for pgSearch: Elasticsearch document field {@code
 * _entityType} (see {@link com.linkedin.metadata.service.search.CombinedSearchDocumentBuilder}).
 */
@Slf4j
public final class PostgresSearchEntityType {

  /** Same string as entity search documents and GraphQL entity filters. */
  public static final String DOCUMENT_FIELD_NAME = "_entityType";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private PostgresSearchEntityType() {}

  /**
   * @return entity type string, or empty string when missing or unparsable (column is {@code NOT
   *     NULL})
   */
  @Nonnull
  public static String extractEntityType(@Nullable String documentJson) {
    if (documentJson == null || documentJson.isBlank()) {
      return "";
    }
    try {
      JsonNode root = MAPPER.readTree(documentJson.getBytes(StandardCharsets.UTF_8));
      JsonNode et = root.get(DOCUMENT_FIELD_NAME);
      if (et != null && et.isTextual()) {
        String v = et.asText();
        return v != null ? v : "";
      }
    } catch (Exception e) {
      log.debug("Could not parse entity_type from document: {}", e.toString());
    }
    return "";
  }
}
