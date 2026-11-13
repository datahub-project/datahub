package com.linkedin.metadata.aspect.patch.builder;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.SemanticContent;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import java.util.Collection;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutableTriple;

/**
 * {@link SemanticContentPatchBuilder} that also states what one profile, and the content it is
 * bound to, must look like when the patch is applied. Each expectation is a JSON Patch {@code test}
 * op. A {@code null} value means the path must be absent, which plain JSON Patch cannot express.
 * {@code SemanticContentTemplate} evaluates them against the stored aspect inside the write
 * transaction, so the check is scoped to one profile and ignores writes to sibling profiles.
 */
public class SemanticProfilePatchBuilder extends SemanticContentPatchBuilder {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Nonnull
  public SemanticProfilePatchBuilder expectProfileField(
      @Nonnull String profileKey, @Nonnull String field, @Nullable JsonNode value) {
    return test(
        "/embeddings/" + encodeValue(profileKey) + "/" + encodeValue(field),
        value == null ? NullNode.instance : value);
  }

  @Nonnull
  public SemanticProfilePatchBuilder expectProfileAbsent(@Nonnull String profileKey) {
    return test("/embeddings/" + encodeValue(profileKey), NullNode.instance);
  }

  @Nonnull
  public SemanticProfilePatchBuilder expectSource(@Nonnull String watchKey, long timeMillis) {
    return test(sourcePath(watchKey), LongNode.valueOf(timeMillis));
  }

  @Nonnull
  public SemanticProfilePatchBuilder expectSourceAbsent(@Nonnull String watchKey) {
    return test(sourcePath(watchKey), NullNode.instance);
  }

  /**
   * Expects each named field to hold the value read from {@code stored}, or to be absent when the
   * read profile did not have it. A missing profile is expected to stay missing.
   */
  @Nonnull
  public SemanticProfilePatchBuilder expectProfileState(
      @Nonnull String profileKey,
      @Nullable EmbeddingModelData stored,
      @Nonnull Collection<String> fields) {
    if (stored == null) {
      return expectProfileAbsent(profileKey);
    }
    JsonNode node = toJson(stored);
    for (String field : fields) {
      expectProfileField(profileKey, field, node.has(field) ? node.get(field) : null);
    }
    return this;
  }

  /** Expects the entity marker for each watch key to hold the value read from {@code stored}. */
  @Nonnull
  public SemanticProfilePatchBuilder expectBoundSources(
      @Nullable SemanticContent stored, @Nonnull Collection<String> watchKeys) {
    for (String key : watchKeys) {
      Long time =
          stored != null && stored.hasRefreshMarker()
              ? stored.getRefreshMarker().getSources().get(key)
              : null;
      if (time == null) {
        expectSourceAbsent(key);
      } else {
        expectSource(key, time);
      }
    }
    return this;
  }

  @Nonnull
  private SemanticProfilePatchBuilder test(@Nonnull String path, @Nonnull JsonNode value) {
    pathValues.add(ImmutableTriple.of(PatchOperationType.TEST.getValue(), path, value));
    return this;
  }

  @Nonnull
  private static String sourcePath(@Nonnull String watchKey) {
    return "/refreshMarker/sources/" + encodeValue(watchKey);
  }

  @Nonnull
  private static JsonNode toJson(@Nonnull EmbeddingModelData profile) {
    try {
      return MAPPER.readTree(RecordUtils.toJsonString(profile));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize embedding profile", e);
    }
  }
}
