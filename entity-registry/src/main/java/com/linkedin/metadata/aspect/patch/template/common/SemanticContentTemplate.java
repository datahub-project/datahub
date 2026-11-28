package com.linkedin.metadata.aspect.patch.template.common;

import static com.linkedin.metadata.aspect.patch.template.TemplateUtil.OBJECT_MAPPER;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.EmbeddingModelDataMap;
import com.linkedin.common.SemanticContent;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.metadata.aspect.patch.template.Template;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import jakarta.json.JsonPatch;
import jakarta.json.JsonValue;
import java.util.Comparator;
import javax.annotation.Nonnull;

/**
 * Enables JSON Patch on {@code semanticContent}. The embeddings map is patched by key, so a write
 * of one profile leaves the others in place.
 *
 * <p>{@code test} ops under {@code /embeddings/} and {@code /refreshMarker/sources/} are profile
 * preconditions, handled through {@link #preprocessPatch}. They are checked against the stored
 * aspect before any other op runs. This differs from RFC 6902: a {@code null} value means the path
 * must be absent, which the RFC cannot express because it fails a {@code test} on a missing path. A
 * mismatch throws {@link SemanticProfileConflictException} and nothing is written. A {@code test}
 * op elsewhere is standard.
 *
 * <p>See "Test" in {@code docs/advanced/patch.md}.
 */
public class SemanticContentTemplate implements Template<SemanticContent> {

  /**
   * Numbers compare by value, so 1 and 1.0 match. Other unequal nodes order by their JSON text, so
   * the sign flips when the arguments are swapped, as a comparator must.
   */
  static final Comparator<JsonNode> NUMERIC_AWARE =
      (left, right) -> {
        if (left.isNumber() && right.isNumber()) {
          return left.decimalValue().compareTo(right.decimalValue());
        }
        if (left.equals(right)) {
          return 0;
        }
        return left.toString().compareTo(right.toString());
      };

  @Override
  public SemanticContent getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof SemanticContent) {
      return (SemanticContent) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to SemanticContent");
  }

  @Override
  public Class<SemanticContent> getTemplateType() {
    return SemanticContent.class;
  }

  @Nonnull
  @Override
  public SemanticContent getDefault() {
    return new SemanticContent().setEmbeddings(new EmbeddingModelDataMap());
  }

  @Override
  public JsonPatch preprocessPatch(@Nonnull JsonNode stored, @Nonnull JsonPatch jsonPatch)
      throws JsonProcessingException {
    JsonArrayBuilder remaining = Json.createArrayBuilder();
    for (JsonValue value : jsonPatch.toJsonArray()) {
      JsonObject op = value.asJsonObject();
      String path = op.getString("path", "");
      if (PatchOperationType.TEST.getValue().equals(op.getString("op", ""))
          && isProfilePrecondition(path)) {
        checkPrecondition(stored, path, op.get("value"));
      } else {
        remaining.add(value);
      }
    }
    return Json.createPatch(remaining.build());
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    return baseNode;
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    return patched;
  }

  private static boolean isProfilePrecondition(@Nonnull String path) {
    return path.startsWith("/embeddings/") || path.startsWith("/refreshMarker/sources/");
  }

  private static void checkPrecondition(
      @Nonnull JsonNode stored, @Nonnull String path, JsonValue expectedValue)
      throws JsonProcessingException {
    JsonNode actual = stored.at(JsonPointer.compile(path));
    boolean actualAbsent = actual.isMissingNode() || actual.isNull();
    if (expectedValue == null || expectedValue.getValueType() == JsonValue.ValueType.NULL) {
      if (!actualAbsent) {
        throw new SemanticProfileConflictException(path);
      }
      return;
    }
    JsonNode expected = OBJECT_MAPPER.readTree(expectedValue.toString());
    if (actualAbsent || !actual.equals(NUMERIC_AWARE, expected)) {
      throw new SemanticProfileConflictException(path);
    }
  }
}
