package com.linkedin.metadata.search.postgres;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MappingConstants;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Builds {@code systemmetadata} JSONB for pgSearch: per-aspect {@link
 * com.linkedin.mxe.SystemMetadata} objects keyed by aspect name (same layout as {@code _aspects} in
 * the Elasticsearch v3 entity document, without other aspect fields).
 */
@Slf4j
public final class PostgresSearchSystemMetadataJson {

  /** Matches {@link com.linkedin.metadata.service.search.CombinedSearchDocumentBuilder}. */
  private static final String ASPECT_SYSTEM_METADATA_FIELD = "_systemmetadata";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private PostgresSearchSystemMetadataJson() {}

  /**
   * Computes {@code systemmetadata} for an upsert. For each aspect in the new document's {@code
   * _aspects}, uses {@code _systemmetadata} from the document when present; otherwise reuses the
   * value from {@code existingSystemMetadataColumnJson} for that aspect name (so a later document
   * version can omit per-aspect {@code _systemmetadata} without losing the stored column). Aspects
   * removed from {@code _aspects} are dropped from the result.
   *
   * <p>If the new document has no {@code _aspects} object, returns {@code
   * existingSystemMetadataColumnJson} unchanged (including {@code null}).
   */
  @Nullable
  public static String mergeAspectSystemMetadataForUpsert(
      @Nullable String existingSystemMetadataColumnJson, @Nonnull String newDocumentJson) {
    try {
      JsonNode root = MAPPER.readTree(newDocumentJson.getBytes(StandardCharsets.UTF_8));
      JsonNode aspects = root.get(MappingConstants.ASPECTS_FIELD_NAME);
      if (aspects == null || !aspects.isObject()) {
        return emptyToNull(existingSystemMetadataColumnJson);
      }

      JsonNode existingParsed = null;
      if (existingSystemMetadataColumnJson != null && !existingSystemMetadataColumnJson.isBlank()) {
        JsonNode rawExisting =
            MAPPER.readTree(existingSystemMetadataColumnJson.getBytes(StandardCharsets.UTF_8));
        if (rawExisting.isObject()) {
          existingParsed = rawExisting;
        }
      }
      final JsonNode existingForMerge = existingParsed;

      ObjectNode out = MAPPER.createObjectNode();
      aspects
          .fieldNames()
          .forEachRemaining(
              aspectName -> {
                JsonNode aspectDoc = aspects.get(aspectName);
                if (aspectDoc == null || !aspectDoc.isObject()) {
                  return;
                }
                JsonNode sm = aspectDoc.get(ASPECT_SYSTEM_METADATA_FIELD);
                if (sm != null && !sm.isNull()) {
                  out.set(aspectName, sm);
                } else if (existingForMerge != null && existingForMerge.has(aspectName)) {
                  out.set(aspectName, existingForMerge.get(aspectName));
                }
              });

      if (out.isEmpty()) {
        return null;
      }
      return MAPPER.writeValueAsString(out);
    } catch (Exception e) {
      log.debug("Could not merge systemmetadata for upsert: {}", e.toString());
      return buildAspectSystemMetadataPayload(newDocumentJson);
    }
  }

  @Nullable
  private static String emptyToNull(@Nullable String s) {
    if (s == null || s.isBlank()) {
      return null;
    }
    return s;
  }

  /**
   * @return JSON string for binding to {@code systemmetadata}, or {@code null} when no aspect
   *     carries system metadata
   */
  @Nullable
  public static String buildAspectSystemMetadataPayload(@Nullable String documentJson) {
    if (documentJson == null || documentJson.isBlank()) {
      return null;
    }
    try {
      JsonNode root = MAPPER.readTree(documentJson.getBytes(StandardCharsets.UTF_8));
      JsonNode aspects = root.get(MappingConstants.ASPECTS_FIELD_NAME);
      if (aspects == null || !aspects.isObject()) {
        return null;
      }
      ObjectNode out = MAPPER.createObjectNode();
      aspects
          .fieldNames()
          .forEachRemaining(
              aspectName -> {
                JsonNode aspectDoc = aspects.get(aspectName);
                if (aspectDoc != null && aspectDoc.isObject()) {
                  JsonNode sm = aspectDoc.get(ASPECT_SYSTEM_METADATA_FIELD);
                  if (sm != null && !sm.isNull()) {
                    out.set(aspectName, sm);
                  }
                }
              });
      if (out.isEmpty()) {
        return null;
      }
      return MAPPER.writeValueAsString(out);
    } catch (Exception e) {
      log.debug("Could not derive systemmetadata from document: {}", e.toString());
      return null;
    }
  }
}
