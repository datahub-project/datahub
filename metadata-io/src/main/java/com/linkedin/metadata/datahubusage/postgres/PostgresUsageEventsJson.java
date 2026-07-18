package com.linkedin.metadata.datahubusage.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.metadata.datahubusage.DataHubUsageEventConstants;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Instant;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class PostgresUsageEventsJson {

  private static final Pattern ISO8601_PATTERN = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}T.*");

  /**
   * Build an insert row from the MAE transformer output ({@code document} JSON string) and Kafka
   * document id (with offset suffix applied by the MAE processor).
   *
   * @param operationContext supplies the shared {@code ObjectMapper} (stream limits, modules,
   *     etc.).
   */
  public static PostgresUsageEventInsertRow parse(
      @Nonnull OperationContext operationContext, String documentId, String documentJsonString)
      throws JsonProcessingException {
    JsonNode root = operationContext.getObjectMapper().readTree(documentJsonString);

    JsonNode timestampNode = root.get(DataHubUsageEventConstants.TIMESTAMP);
    if (timestampNode == null || timestampNode.isNull()) {
      throw new JsonProcessingException("Missing timestamp field") {};
    }

    final long timestampMs;
    if (timestampNode.isNumber()) {
      timestampMs = timestampNode.asLong();
    } else if (timestampNode.isTextual()) {
      String textValue = timestampNode.asText("");
      timestampMs =
          ISO8601_PATTERN.matcher(textValue).matches()
              ? Instant.parse(textValue).toEpochMilli()
              : Long.parseLong(textValue);
    } else {
      throw new JsonProcessingException("Invalid timestamp type") {};
    }

    return PostgresUsageEventInsertRow.builder()
        .id(documentId)
        .timestampMs(timestampMs)
        .eventType(readText(root.get(DataHubUsageEventConstants.TYPE)))
        .usageSource(readText(root.get(DataHubUsageEventConstants.USAGE_SOURCE)))
        .actorUrn(readText(root.get(DataHubUsageEventConstants.ACTOR_URN)))
        .entityUrn(readText(root.get(DataHubUsageEventConstants.ENTITY_URN)))
        .entityType(readText(root.get(DataHubUsageEventConstants.ENTITY_TYPE)))
        .browserId(readText(root.get("browserId")))
        .query(readText(root.get(DataHubUsageEventConstants.QUERY)))
        .section(readText(root.get("section")))
        .actionType(readText(root.get("actionType")))
        .aspectName(readText(root.get(DataHubUsageEventConstants.ASPECT_NAME)))
        .documentJson(documentJsonString)
        .build();
  }

  @Nullable
  private static String readText(@Nullable JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isValueNode()) {
      return null;
    }
    String s = node.asText();
    return s.isBlank() ? null : s;
  }
}
