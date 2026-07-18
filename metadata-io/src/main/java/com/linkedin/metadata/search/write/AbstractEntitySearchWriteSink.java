package com.linkedin.metadata.search.write;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** Shared helpers for interpreting ES-shaped entity search JSON before binding JDBC parameters. */
@Slf4j
public abstract class AbstractEntitySearchWriteSink implements EntitySearchWriteSink {

  /** Same cap as {@link com.linkedin.metadata.search.elasticsearch.ElasticSearchService}. */
  public static final int MAX_RUN_IDS_INDEXED = 25;

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Merges {@code runId} into a search document's {@code runId} list with ES painless semantics:
   * unique, ordered, capped at {@link #MAX_RUN_IDS_INDEXED}.
   */
  @Nonnull
  public static String mergeRunIdIntoDocumentJson(
      @Nullable String existingDocumentJson, @Nonnull String urnString, @Nullable String runId) {
    if (runId == null || runId.isBlank()) {
      return existingDocumentJson != null && !existingDocumentJson.isBlank()
          ? existingDocumentJson
          : "{}";
    }
    try {
      ObjectNode root;
      if (existingDocumentJson == null || existingDocumentJson.isBlank()) {
        root = MAPPER.createObjectNode();
        root.put("urn", urnString);
      } else {
        JsonNode parsed = MAPPER.readTree(existingDocumentJson.getBytes(StandardCharsets.UTF_8));
        root = parsed.isObject() ? (ObjectNode) parsed : MAPPER.createObjectNode();
      }
      ArrayNode runIds;
      if (root.has("runId") && root.get("runId").isArray()) {
        runIds = (ArrayNode) root.get("runId");
      } else {
        runIds = MAPPER.createArrayNode();
        root.set("runId", runIds);
      }
      for (JsonNode n : runIds) {
        if (runId.equals(n.asText())) {
          return MAPPER.writeValueAsString(root);
        }
      }
      runIds.add(runId);
      while (runIds.size() > MAX_RUN_IDS_INDEXED) {
        runIds.remove(0);
      }
      return MAPPER.writeValueAsString(root);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid search document JSON for runId merge", e);
    }
  }

  /**
   * Derives plain text for lexical {@code tsvector} from the search document. Prefer root {@code
   * name}, then common description fields, then the URN string.
   */
  @Nonnull
  protected static String deriveSearchText(@Nonnull String urn, @Nonnull String documentJson) {
    try {
      JsonNode root = MAPPER.readTree(documentJson.getBytes(StandardCharsets.UTF_8));
      if (root.has("name") && root.get("name").isTextual()) {
        String n = root.get("name").asText();
        if (!n.isBlank()) {
          return n;
        }
      }
      if (root.has("description") && root.get("description").isTextual()) {
        String d = root.get("description").asText();
        if (!d.isBlank()) {
          return d;
        }
      }
      if (root.has("editedDescription") && root.get("editedDescription").isTextual()) {
        String d = root.get("editedDescription").asText();
        if (!d.isBlank()) {
          return d;
        }
      }
    } catch (Exception e) {
      log.debug(
          "Could not parse search document for lexical text; using urn only: {}", e.toString());
    }
    return urn;
  }
}
