package com.linkedin.metadata.search.postgres;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MappingConstants;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Aggregates plain text from combined entity search JSON ({@code _aspects}) into per-tier strings
 * for {@code search_vector_tierN} columns. Uses {@link SearchableFieldSpec} {@code searchTier} from
 * the entity registry; logical tiers above the configured column count map to the last tier.
 */
@Slf4j
public final class PostgresSearchTierTextAggregator {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private PostgresSearchTierTextAggregator() {}

  /**
   * @param tierColumnCount number of configured {@code search_vector_tierN} columns (>= 1)
   * @return one plain-text string per tier (same length as {@code tierColumnCount}), suitable for
   *     {@code to_tsvector}
   */
  @Nonnull
  public static String[] deriveTierPlainTexts(
      @Nonnull EntityRegistry registry, @Nonnull String documentJson, int tierColumnCount) {
    if (tierColumnCount < 1) {
      throw new IllegalArgumentException("tierColumnCount must be >= 1");
    }
    StringBuilder[] builders = new StringBuilder[tierColumnCount];
    for (int i = 0; i < tierColumnCount; i++) {
      builders[i] = new StringBuilder();
    }
    try {
      JsonNode root = MAPPER.readTree(documentJson.getBytes(StandardCharsets.UTF_8));
      String entityType = readEntityType(root);
      if (entityType == null || entityType.isBlank()) {
        return toTierStrings(builders);
      }
      EntitySpec entitySpec;
      try {
        entitySpec = registry.getEntitySpec(entityType);
      } catch (Exception e) {
        log.debug("Unknown entity type {} for pgSearch tier text: {}", entityType, e.toString());
        return toTierStrings(builders);
      }
      JsonNode aspects = root.get(MappingConstants.ASPECTS_FIELD_NAME);
      if (aspects == null || !aspects.isObject()) {
        return toTierStrings(builders);
      }
      Iterator<String> aspectNames = aspects.fieldNames();
      while (aspectNames.hasNext()) {
        String aspectName = aspectNames.next();
        JsonNode aspectNode = aspects.get(aspectName);
        if (aspectNode == null || !aspectNode.isObject()) {
          continue;
        }
        AspectSpec aspectSpec = entitySpec.getAspectSpec(aspectName);
        if (aspectSpec == null) {
          continue;
        }
        for (SearchableFieldSpec fieldSpec : aspectSpec.getSearchableFieldSpecs()) {
          if (fieldSpec.getSearchableAnnotation().getSearchTier().isEmpty()) {
            continue;
          }
          int logicalTier = fieldSpec.getSearchableAnnotation().getSearchTier().get();
          int clamped = Math.min(Math.max(1, logicalTier), tierColumnCount);
          String fieldName = fieldSpec.getSearchableAnnotation().getFieldName();
          appendJsonLexicalText(builders[clamped - 1], aspectNode.get(fieldName));
        }
      }
    } catch (IOException e) {
      log.debug("Invalid search document JSON for tier aggregation: {}", e.toString());
    }
    return toTierStrings(builders);
  }

  private static String readEntityType(JsonNode root) {
    JsonNode et = root.get("_entityType");
    if (et != null && et.isTextual()) {
      String t = et.asText();
      if (!t.isBlank()) {
        return t;
      }
    }
    JsonNode urnNode = root.get("urn");
    if (urnNode != null && urnNode.isTextual()) {
      try {
        return Urn.createFromString(urnNode.asText()).getEntityType();
      } catch (Exception ignored) {
        return null;
      }
    }
    return null;
  }

  private static void appendJsonLexicalText(StringBuilder sb, JsonNode node) {
    if (node == null || node.isNull() || node.isMissingNode()) {
      return;
    }
    if (node.isTextual()) {
      appendToken(sb, node.asText());
    } else if (node.isNumber() || node.isBoolean()) {
      appendToken(sb, node.asText());
    } else if (node.isArray()) {
      for (JsonNode el : node) {
        appendJsonLexicalText(sb, el);
      }
    } else if (node.isObject()) {
      node.elements().forEachRemaining(v -> appendJsonLexicalText(sb, v));
    }
  }

  private static void appendToken(StringBuilder sb, String token) {
    if (token == null || token.isBlank()) {
      return;
    }
    if (sb.length() > 0) {
      sb.append(' ');
    }
    sb.append(token.trim());
  }

  @Nonnull
  private static String[] toTierStrings(StringBuilder[] builders) {
    String[] out = new String[builders.length];
    for (int i = 0; i < builders.length; i++) {
      out[i] = builders[i].toString().trim();
    }
    return out;
  }
}
