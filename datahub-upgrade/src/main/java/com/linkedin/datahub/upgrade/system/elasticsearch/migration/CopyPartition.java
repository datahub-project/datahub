package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * One unit of the copy: the documents of {@code sourceIndex} that match {@code filter}, written to
 * {@code targetIndex}. Partitions are planned once, persisted on the migration row as JSON, and
 * each is copied to completion independently; a retry re-runs the whole partition, which the write
 * semantics below make idempotent.
 *
 * @param id stable position in the plan, the key of its state entries
 * @param label what the partition covers, for logs and the verify report
 * @param filter a query body restricting the source, or null for the whole index
 * @param estimatedDocs the source count when the plan was made; informational
 * @param guardField null to create only, leaving documents the target already has as they are;
 *     otherwise the source's document replaces the target's unless the target's value of this field
 *     is newer, which lets a mutable family's windowed pass repair a lost update without undoing a
 *     later mirrored write
 */
public record CopyPartition(
    int id,
    @Nonnull String sourceIndex,
    @Nonnull String targetIndex,
    @Nonnull String label,
    @Nullable JsonNode filter,
    long estimatedDocs,
    @Nullable String guardField) {

  @Nonnull
  public String toJson(@Nonnull final ObjectMapper mapper) {
    final ObjectNode node = mapper.createObjectNode();
    node.put("id", id);
    node.put("sourceIndex", sourceIndex);
    node.put("targetIndex", targetIndex);
    node.put("label", label);
    if (filter != null) {
      node.set("filter", filter);
    }
    node.put("estimatedDocs", estimatedDocs);
    if (guardField != null) {
      node.put("guardField", guardField);
    }
    try {
      return mapper.writeValueAsString(node);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Nonnull
  public static CopyPartition fromJson(
      @Nonnull final ObjectMapper mapper, @Nonnull final String json) {
    try {
      final JsonNode node = mapper.readTree(json);
      return new CopyPartition(
          node.path("id").asInt(),
          node.path("sourceIndex").asText(),
          node.path("targetIndex").asText(),
          node.path("label").asText(),
          node.has("filter") ? node.get("filter") : null,
          node.path("estimatedDocs").asLong(),
          node.has("guardField") ? node.get("guardField").asText() : null);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
