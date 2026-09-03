package com.linkedin.metadata.timeseries.postgres;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.data.ByteString;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.MappingsBuilder;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/** Maps `{prefix}_aspect` columns to {@link EnvelopedAspect} (parity with ES parseDocument). */
@Slf4j
public final class TimeseriesPgDocumentMapper {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Column bundle for {@code {prefix}_aspect} SqlSetup tables. */
  @Value
  public static class TimeseriesAspectRowPayload {
    @Nonnull String entityName;
    @Nonnull String aspectName;
    @Nonnull String urn;
    @Nonnull String messageId;
    long timestampMillis;
    String runId;
    String eventGranularity;
    String partitionSpecJson;
    String eventJson;
    String systemMetadataJson;
    @Nonnull String documentJson;
  }

  /**
   * Same field set as {@link
   * com.linkedin.metadata.timeseries.elastic.ElasticSearchTimeseriesAspectService} non-exploded
   * document routing — used to build a synthetic {@link MappingsBuilder#EVENT_FIELD} when only
   * collection-exploded rows exist (no top-level {@code event}).
   */
  private static final Set<String> TIMESERIES_DOC_COMMON_FIELD_NAMES =
      Set.of(
          MappingsBuilder.URN_FIELD,
          MappingsBuilder.RUN_ID_FIELD,
          MappingsBuilder.EVENT_GRANULARITY,
          MappingsBuilder.IS_EXPLODED_FIELD,
          MappingsBuilder.MESSAGE_ID_FIELD,
          MappingsBuilder.PARTITION_SPEC_PARTITION,
          MappingsBuilder.PARTITION_SPEC,
          MappingsBuilder.SYSTEM_METADATA_FIELD,
          MappingsBuilder.TIMESTAMP_MILLIS_FIELD,
          MappingsBuilder.TIMESTAMP_FIELD,
          MappingsBuilder.EVENT_FIELD);

  private TimeseriesPgDocumentMapper() {}

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
    String partitionSpecJson = serializeOptional(document, MappingsBuilder.PARTITION_SPEC, false);
    String eventJson = serializeOptional(document, MappingsBuilder.EVENT_FIELD, true);
    String systemMetadataJson =
        serializeOptional(document, MappingsBuilder.SYSTEM_METADATA_FIELD, false);
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
        resolveMessageId(docId, document),
        tsNode.asLong(),
        document.hasNonNull(MappingsBuilder.RUN_ID_FIELD)
            ? document.get(MappingsBuilder.RUN_ID_FIELD).asText()
            : null,
        document.hasNonNull(MappingsBuilder.EVENT_GRANULARITY)
            ? document.get(MappingsBuilder.EVENT_GRANULARITY).asText()
            : null,
        partitionSpecJson,
        eventJson,
        systemMetadataJson,
        documentJson);
  }

  @Nonnull
  public static String resolveMessageId(@Nonnull String docId, @Nullable JsonNode document) {
    if (document != null && document.hasNonNull(MappingsBuilder.MESSAGE_ID_FIELD)) {
      return document.get(MappingsBuilder.MESSAGE_ID_FIELD).asText();
    }
    return docId;
  }

  @Nullable
  private static String serializeOptional(
      @Nonnull JsonNode document, @Nonnull String field, boolean failOnError) {
    JsonNode value = document.get(field);
    if (value == null || value.isNull()) {
      return null;
    }
    try {
      return MAPPER.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      if (failOnError) {
        throw new IllegalArgumentException("Failed to serialize " + field + " field", e);
      }
      log.warn("Failed to serialize {}: {}", field, e.toString());
      return null;
    }
  }

  private static String requiredText(@Nonnull JsonNode document, @Nonnull String field) {
    JsonNode value = document.get(field);
    if (value == null || value.isNull() || !value.isTextual()) {
      throw new IllegalArgumentException("timeseries document missing required field: " + field);
    }
    return value.asText();
  }

  @Nonnull
  public static EnvelopedAspect envelopedAspectFromRow(
      @Nonnull OperationContext opContext, @Nonnull ResultSet rs, boolean preferDocumentColumn)
      throws SQLException {

    String eventJson = rs.getString("event");
    String sysMetaJson = rs.getString("system_metadata");
    if (preferDocumentColumn) {
      String docJson = rs.getString("document");
      if (docJson != null && !docJson.isBlank()) {
        return envelopedAspectFromDocumentJson(opContext, docJson);
      }
    }

    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    ObjectMapper mapper = opContext.getObjectMapper();
    try {
      Object event =
          eventJson != null && !eventJson.isBlank()
              ? mapper.readTree(eventJson)
              : mapper.createObjectNode();
      GenericAspect genericAspect =
          new GenericAspect()
              .setValue(
                  ByteString.unsafeWrap(
                      mapper.writeValueAsString(event).getBytes(StandardCharsets.UTF_8)));
      genericAspect.setContentType("application/json");
      envelopedAspect.setAspect(genericAspect);

      if (sysMetaJson != null && !sysMetaJson.isBlank()) {
        envelopedAspect.setSystemMetadata(
            RecordUtils.toRecordTemplate(SystemMetadata.class, sysMetaJson));
      }
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to map PostgreSQL timeseries row to aspect", e);
    }
    return envelopedAspect;
  }

  @Nonnull
  private static EnvelopedAspect envelopedAspectFromDocumentJson(
      @Nonnull OperationContext opContext, @Nonnull String documentJson) throws SQLException {
    try {
      com.fasterxml.jackson.databind.JsonNode doc =
          opContext.getObjectMapper().readTree(documentJson);
      EnvelopedAspect envelopedAspect = new EnvelopedAspect();
      com.fasterxml.jackson.databind.JsonNode eventNode = doc.get(MappingsBuilder.EVENT_FIELD);
      if (eventNode == null || eventNode.isNull()) {
        com.fasterxml.jackson.databind.node.ObjectNode synthetic =
            opContext.getObjectMapper().createObjectNode();
        Iterator<String> fieldNames = doc.fieldNames();
        while (fieldNames.hasNext()) {
          String name = fieldNames.next();
          if (!TIMESERIES_DOC_COMMON_FIELD_NAMES.contains(name)) {
            synthetic.set(name, doc.get(name));
          }
        }
        eventNode = synthetic;
      }
      GenericAspect genericAspect =
          new GenericAspect()
              .setValue(
                  ByteString.unsafeWrap(
                      opContext
                          .getObjectMapper()
                          .writeValueAsString(eventNode)
                          .getBytes(StandardCharsets.UTF_8)));
      genericAspect.setContentType("application/json");
      envelopedAspect.setAspect(genericAspect);
      com.fasterxml.jackson.databind.JsonNode sm = doc.get(MappingsBuilder.SYSTEM_METADATA_FIELD);
      if (sm != null && !sm.isNull()) {
        envelopedAspect.setSystemMetadata(
            RecordUtils.toRecordTemplate(
                SystemMetadata.class, opContext.getObjectMapper().writeValueAsString(sm)));
      }
      return envelopedAspect;
    } catch (JsonProcessingException e) {
      throw new SQLException("Failed to parse document jsonb", e);
    }
  }

  /**
   * Raw document map for {@link com.linkedin.metadata.timeseries.TimeseriesAspectService#raw} —
   * parses full {@code document} jsonb into a shallow Map structure via Jackson.
   */
  @Nullable
  public static java.util.Map<String, Object> rawDocumentMap(
      @Nonnull ObjectMapper mapper, @Nullable String documentJson) {
    if (documentJson == null || documentJson.isBlank()) {
      return null;
    }
    try {
      return mapper.readValue(documentJson, new TypeReference<Map<String, Object>>() {});
    } catch (JsonProcessingException e) {
      log.warn("Could not parse timeseries document json to map: {}", e.toString());
      return null;
    }
  }
}
