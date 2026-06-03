package com.linkedin.metadata.search.elasticsearch.client.shim.impl.v8;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import javax.annotation.Nonnull;

/**
 * Rewrites legacy OpenSearch {@code RangeQueryBuilder} JSON ({@code from}/{@code to} with {@code
 * include_lower}/{@code include_upper}) into ES 8-compatible bounds ({@code gte}/{@code gt}/{@code
 * lte}/{@code lt}). OpenSearch HLRC {@code QueryBuilder#toString()} still emits the legacy shape,
 * which Elasticsearch 8.18+ deprecates.
 */
public final class LegacyRangeQueryNormalizer {

  private LegacyRangeQueryNormalizer() {}

  @Nonnull
  public static String normalize(@Nonnull String queryJson, @Nonnull ObjectMapper objectMapper)
      throws JsonProcessingException {
    JsonNode root = objectMapper.readTree(queryJson);
    normalizeRangeNodes(root);
    return objectMapper.writeValueAsString(root);
  }

  private static void normalizeRangeNodes(JsonNode node) {
    if (node == null) {
      return;
    }
    if (node.isObject()) {
      ObjectNode objectNode = (ObjectNode) node;
      if (objectNode.has("range") && objectNode.get("range").isObject()) {
        ObjectNode rangeNode = (ObjectNode) objectNode.get("range");
        rangeNode
            .properties()
            .forEach(
                entry -> {
                  if (entry.getValue().isObject()) {
                    normalizeRangeSpec((ObjectNode) entry.getValue());
                  }
                });
      }
      objectNode.properties().forEach(entry -> normalizeRangeNodes(entry.getValue()));
    } else if (node.isArray()) {
      node.forEach(LegacyRangeQueryNormalizer::normalizeRangeNodes);
    }
  }

  private static void normalizeRangeSpec(ObjectNode spec) {
    if (!spec.has("from") && !spec.has("to")) {
      return;
    }

    JsonNode from = spec.get("from");
    JsonNode to = spec.get("to");
    boolean includeLower = !spec.has("include_lower") || spec.get("include_lower").asBoolean(true);
    boolean includeUpper = !spec.has("include_upper") || spec.get("include_upper").asBoolean(true);

    if (from != null && !from.isNull()) {
      spec.set(includeLower ? "gte" : "gt", from);
    }
    if (to != null && !to.isNull()) {
      spec.set(includeUpper ? "lte" : "lt", to);
    }

    spec.remove("from");
    spec.remove("to");
    spec.remove("include_lower");
    spec.remove("include_upper");
  }
}
