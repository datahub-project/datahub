package com.linkedin.metadata.queue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Serializes Kafka-style headers to PostgreSQL {@code jsonb}: {@code [{"key":"…","v":"<base64>"}]}.
 * Order matches the producer list (Kafka preserves header order).
 */
public final class QueueHeadersJson {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private QueueHeadersJson() {}

  /**
   * @return {@code null} when there are no headers (SQL {@code NULL}).
   */
  @Nullable
  public static String serialize(@Nonnull List<QueueMessageHeader> headers) {
    if (headers.isEmpty()) {
      return null;
    }
    ArrayNode arr = MAPPER.createArrayNode();
    Base64.Encoder enc = Base64.getEncoder();
    for (QueueMessageHeader h : headers) {
      ObjectNode o = MAPPER.createObjectNode();
      o.put("key", h.key());
      o.put("v", enc.encodeToString(h.value()));
      arr.add(o);
    }
    return arr.toString();
  }

  @Nonnull
  public static List<QueueMessageHeader> deserialize(@Nullable Object pgJsonb) throws SQLException {
    if (pgJsonb == null) {
      return List.of();
    }
    String raw = pgJsonb instanceof String s ? s : pgJsonb.toString();
    if (raw == null || raw.isBlank()) {
      return List.of();
    }
    try {
      JsonNode root = MAPPER.readTree(raw);
      if (!root.isArray()) {
        return List.of();
      }
      Base64.Decoder dec = Base64.getDecoder();
      List<QueueMessageHeader> out = new ArrayList<>();
      for (JsonNode n : root) {
        String key = n.path("key").asText("");
        String vb64 = n.path("v").asText("");
        out.add(new QueueMessageHeader(key, dec.decode(vb64)));
      }
      return Collections.unmodifiableList(out);
    } catch (Exception e) {
      throw new SQLException("Invalid pgQueue headers JSON", e);
    }
  }
}
