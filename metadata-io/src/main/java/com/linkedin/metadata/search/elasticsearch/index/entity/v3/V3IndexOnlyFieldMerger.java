package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.search.utils.ESUtils.SYSTEM_CREATED_FIELD;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.mxe.SystemMetadata;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Preserves operational fields that are not derived from latest aspect projection. */
public class V3IndexOnlyFieldMerger {
  public static final String RUN_ID_FIELD = "runId";
  public static final String SYSTEM_INDEX_MODIFIED_FIELD = "systemIndexModified";
  public static final int MAX_RUN_IDS_INDEXED = 25;

  private final LongSupplier nowMillis;

  public V3IndexOnlyFieldMerger() {
    this(System::currentTimeMillis);
  }

  V3IndexOnlyFieldMerger(@Nonnull final LongSupplier nowMillis) {
    this.nowMillis = nowMillis;
  }

  /**
   * Marks the document as written now. A live partial update uses this instead of {@link #merge}:
   * writing {@code runId} would replace the stored array, so those ids are appended separately.
   */
  public void stampIndexModified(@Nonnull final ObjectNode document) {
    document.put(SYSTEM_INDEX_MODIFIED_FIELD, nowMillis.getAsLong());
  }

  public void merge(
      @Nonnull final ObjectNode document,
      @Nonnull final Optional<Map<String, Object>> existingSource,
      @Nonnull final Collection<String> currentRunIds,
      @Nonnull final Collection<SystemMetadata> latestSystemMetadata) {
    mergeRunIds(document, existingSource, currentRunIds, latestSystemMetadata);
    mergeSystemCreated(document, existingSource);
    document.put(SYSTEM_INDEX_MODIFIED_FIELD, nowMillis.getAsLong());
  }

  private static void mergeRunIds(
      @Nonnull final ObjectNode document,
      @Nonnull final Optional<Map<String, Object>> existingSource,
      @Nonnull final Collection<String> currentRunIds,
      @Nonnull final Collection<SystemMetadata> latestSystemMetadata) {
    final Set<String> orderedRunIds = new LinkedHashSet<>();
    existingSource
        .map(source -> source.get(RUN_ID_FIELD))
        .ifPresent(value -> addRunIds(value, orderedRunIds));
    latestSystemMetadata.forEach(metadata -> addSystemMetadataRunIds(metadata, orderedRunIds));
    currentRunIds.forEach(runId -> addRunId(runId, orderedRunIds));

    if (orderedRunIds.isEmpty()) {
      document.remove(RUN_ID_FIELD);
      return;
    }

    final List<String> cappedRunIds = capOldest(orderedRunIds);
    final ArrayNode runIdArray = JsonNodeFactory.instance.arrayNode();
    cappedRunIds.forEach(runIdArray::add);
    document.set(RUN_ID_FIELD, runIdArray);
  }

  private static void mergeSystemCreated(
      @Nonnull final ObjectNode document,
      @Nonnull final Optional<Map<String, Object>> existingSource) {
    existingSource
        .map(source -> source.get(SYSTEM_CREATED_FIELD))
        .flatMap(V3IndexOnlyFieldMerger::toLong)
        .ifPresent(value -> document.put(SYSTEM_CREATED_FIELD, value));
  }

  private static List<String> capOldest(@Nonnull final Set<String> orderedRunIds) {
    final List<String> runIds = new ArrayList<>(orderedRunIds);
    if (runIds.size() <= MAX_RUN_IDS_INDEXED) {
      return runIds;
    }
    return runIds.subList(runIds.size() - MAX_RUN_IDS_INDEXED, runIds.size());
  }

  private static void addSystemMetadataRunIds(
      @Nullable final SystemMetadata systemMetadata, @Nonnull final Set<String> runIds) {
    if (systemMetadata == null) {
      return;
    }
    if (systemMetadata.hasRunId()) {
      addRunId(systemMetadata.getRunId(), runIds);
    }
    if (systemMetadata.hasLastRunId()) {
      addRunId(systemMetadata.getLastRunId(), runIds);
    }
  }

  private static void addRunIds(@Nullable final Object value, @Nonnull final Set<String> runIds) {
    if (value instanceof Collection<?>) {
      ((Collection<?>) value).forEach(item -> addRunId(item, runIds));
      return;
    }
    if (value instanceof JsonNode) {
      final JsonNode node = (JsonNode) value;
      if (node.isArray()) {
        node.forEach(item -> addRunId(item, runIds));
      } else {
        addRunId(node, runIds);
      }
      return;
    }
    addRunId(value, runIds);
  }

  private static void addRunId(@Nullable final Object value, @Nonnull final Set<String> runIds) {
    if (value == null) {
      return;
    }
    final String runId;
    if (value instanceof JsonNode) {
      final JsonNode node = (JsonNode) value;
      runId = node.isTextual() ? node.asText() : node.toString();
    } else {
      runId = String.valueOf(value);
    }
    if (!runId.isBlank()) {
      // Move re-mentioned runIds to the tail so capping to the newest entries never evicts a
      // runId that is active in the current write just because it also appears in history.
      runIds.remove(runId);
      runIds.add(runId);
    }
  }

  private static Optional<Long> toLong(@Nullable final Object value) {
    if (value instanceof Number) {
      return Optional.of(((Number) value).longValue());
    }
    if (value instanceof JsonNode && ((JsonNode) value).canConvertToLong()) {
      return Optional.of(((JsonNode) value).asLong());
    }
    if (value instanceof String) {
      try {
        return Optional.of(Long.parseLong((String) value));
      } catch (NumberFormatException ignored) {
        return Optional.empty();
      }
    }
    return Optional.empty();
  }
}
