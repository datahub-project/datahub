package com.linkedin.metadata.timeseries.write;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.MappingsBuilder;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Shared parsing from Elasticsearch-shaped timeseries documents (same JSON as {@link
 * com.linkedin.metadata.timeseries.elastic.ElasticSearchTimeseriesAspectService} bulk indexing).
 */
@Slf4j
public abstract class AbstractTimeseriesAspectWriteSink implements TimeseriesAspectWriteSink {

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  /** Column bundle for {@code {prefix}_aspect_row} SqlSetup tables. */
  @Value
  public static class TimeseriesAspectRowPayload {
    @Nonnull String entityName;
    @Nonnull String aspectName;
    @Nonnull String urn;
    @Nonnull String messageId;
    long timestampMillis;
    String runId;
    String eventGranularity;

    /** JSON text for partition_spec jsonb column, or null */
    String partitionSpecJson;

    /** JSON text for event jsonb column, or null */
    String eventJson;

    /** JSON text for system_metadata jsonb column, or null */
    String systemMetadataJson;

    /** Full document JSON (same as ES doc) for document jsonb column */
    @Nonnull String documentJson;
  }

  /**
   * Parses {@code document} using {@link MappingsBuilder} field names; uses {@code docId} as {@link
   * MappingsBuilder#MESSAGE_ID_FIELD} when absent (matches exploded / synthetic ids).
   */
  @Nonnull
  public static TimeseriesAspectRowPayload parsePayload(
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull String docId,
      @Nonnull JsonNode document) {
    String urn = requiredText(document, MappingsBuilder.URN_FIELD);
    JsonNode tsNode = document.get(MappingsBuilder.TIMESTAMP_MILLIS_FIELD);
    if (tsNode == null || !tsNode.isNumber()) {
      throw new IllegalArgumentException("timeseries document missing timestampMillis");
    }
    long timestampMillis = tsNode.asLong();
    String messageId = resolveMessageId(docId, document);
    String runId =
        document.hasNonNull(MappingsBuilder.RUN_ID_FIELD)
            ? document.get(MappingsBuilder.RUN_ID_FIELD).asText()
            : null;
    String eventGranularity =
        document.hasNonNull(MappingsBuilder.EVENT_GRANULARITY)
            ? document.get(MappingsBuilder.EVENT_GRANULARITY).asText()
            : null;
    String partitionSpecJson = null;
    if (document.has(MappingsBuilder.PARTITION_SPEC)
        && !document.get(MappingsBuilder.PARTITION_SPEC).isNull()) {
      try {
        partitionSpecJson = MAPPER.writeValueAsString(document.get(MappingsBuilder.PARTITION_SPEC));
      } catch (JsonProcessingException e) {
        log.warn("Failed to serialize partitionSpec to JSON: {}", e.toString());
      }
    }
    String eventJson = null;
    if (document.has(MappingsBuilder.EVENT_FIELD)) {
      try {
        eventJson = MAPPER.writeValueAsString(document.get(MappingsBuilder.EVENT_FIELD));
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("Failed to serialize event field", e);
      }
    }
    String systemMetadataJson = null;
    if (document.has(MappingsBuilder.SYSTEM_METADATA_FIELD)) {
      try {
        systemMetadataJson =
            MAPPER.writeValueAsString(document.get(MappingsBuilder.SYSTEM_METADATA_FIELD));
      } catch (JsonProcessingException e) {
        log.warn("Failed to serialize systemMetadata: {}", e.toString());
      }
    }
    String documentJson;
    try {
      documentJson = MAPPER.writeValueAsString(document);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize full timeseries document", e);
    }

    return new TimeseriesAspectRowPayload(
        entityName,
        aspectName,
        urn,
        messageId,
        timestampMillis,
        runId,
        eventGranularity,
        partitionSpecJson,
        eventJson,
        systemMetadataJson,
        documentJson);
  }

  /**
   * JDBC {@code message_id}: logical {@link MappingsBuilder#MESSAGE_ID_FIELD} when present in the
   * document, otherwise {@code docId} (Transformer map key / Elasticsearch id).
   */
  @Nonnull
  public static String resolveMessageId(@Nonnull String docId, @Nullable JsonNode document) {
    if (document != null && document.hasNonNull(MappingsBuilder.MESSAGE_ID_FIELD)) {
      return document.get(MappingsBuilder.MESSAGE_ID_FIELD).asText();
    }
    return docId;
  }

  private static String requiredText(JsonNode document, String field) {
    JsonNode n = document.get(field);
    if (n == null || n.isNull() || !n.isTextual()) {
      throw new IllegalArgumentException("timeseries document missing required field: " + field);
    }
    return n.asText();
  }
}
