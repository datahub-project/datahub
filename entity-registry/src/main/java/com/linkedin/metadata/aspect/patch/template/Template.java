package com.linkedin.metadata.aspect.patch.template;

import static com.linkedin.metadata.aspect.patch.template.TemplateUtil.OBJECT_MAPPER;
import static com.linkedin.metadata.aspect.patch.template.TemplateUtil.populateTopLevelKeys;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonPatch;
import java.io.StringReader;
import javax.annotation.Nonnull;

public interface Template<T extends RecordTemplate> {

  /**
   * Cast method to get subtype of {@link RecordTemplate} for applying templating methods
   *
   * @param recordTemplate generic record
   * @return specific type for this template
   * @throws {@link ClassCastException} when recordTemplate is not the correct type for the template
   */
  default T getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (getTemplateType().isInstance(recordTemplate)) {
      return getTemplateType().cast(recordTemplate);
    }
    throw new ClassCastException("Unable to cast RecordTemplate to " + getTemplateType().getName());
  }

  /** Get the template clas type */
  Class<T> getTemplateType();

  /**
   * Get a template aspect with defaults set
   *
   * @return subtype of {@link RecordTemplate} that lines up with a predefined AspectSpec
   */
  @Nonnull
  T getDefault();

  /**
   * Applies a specified {@link Patch} to an aspect
   *
   * @param recordTemplate original {@link RecordTemplate} to be patched
   * @param jsonPatch patch to apply
   * @return patched value
   * @throws JsonProcessingException if there is an issue converting the input to JSON
   */
  default T applyPatch(RecordTemplate recordTemplate, JsonPatch jsonPatch)
      throws JsonProcessingException {
    TemplateUtil.validatePatch(jsonPatch);

    JsonNode stored = preprocessTemplate(recordTemplate);
    JsonPatch effectivePatch = preprocessPatch(stored, jsonPatch);
    JsonNode transformed = populateTopLevelKeys(stored, effectivePatch);
    try {
      // Hack in a more efficient patcher. Even with the serialization overhead 140% faster
      JsonObject patched =
          effectivePatch.apply(
              Json.createReader(new StringReader(OBJECT_MAPPER.writeValueAsString(transformed)))
                  .readObject());
      JsonNode postProcessed = rebaseFields(OBJECT_MAPPER.readTree(patched.toString()));
      return RecordUtils.toRecordTemplate(getTemplateType(), postProcessed.toString());
    } catch (JsonProcessingException | jakarta.json.JsonException e) {
      throw new RuntimeException(
          String.format(
              "Error performing JSON PATCH on aspect %s. Patch: %s Target: %s",
              recordTemplate.schema().getName(), effectivePatch, transformed.toString()),
          e);
    }
  }

  /**
   * Extension point for an aspect whose patch operations deviate from RFC 6902. It runs on the
   * stored aspect, after {@link #transformFields} and before the patch is applied, and returns the
   * patch to apply. An implementation can consume operations it evaluates itself and may throw to
   * reject the patch before anything is applied. The default applies the patch as given, which is
   * the standard behavior.
   *
   * @param stored the stored aspect as patch-compatible json
   * @param jsonPatch the patch as requested
   * @return the patch to apply to {@code stored}
   * @throws JsonProcessingException if there is an issue reading a patch value
   */
  default JsonPatch preprocessPatch(@Nonnull JsonNode stored, @Nonnull JsonPatch jsonPatch)
      throws JsonProcessingException {
    return jsonPatch;
  }

  /**
   * Returns a json representation of the template, modified for template based operations to be
   * compatible with patch semantics.
   *
   * @param recordTemplate template to be transformed into json
   * @return a {@link JsonNode} representation of the template
   * @throws JsonProcessingException if there is an issue converting the input to JSON
   */
  default JsonNode preprocessTemplate(RecordTemplate recordTemplate)
      throws JsonProcessingException {
    T subtype = getSubtype(recordTemplate);
    JsonNode baseNode = OBJECT_MAPPER.readTree(RecordUtils.toJsonString(subtype));
    return transformFields(baseNode);
  }

  /**
   * Transforms fields from base json representation of RecordTemplate to definition specific to
   * aspect per patch semantics
   *
   * @param baseNode the base node to be transformed
   * @return transformed {@link JsonNode}
   */
  @Nonnull
  JsonNode transformFields(JsonNode baseNode);

  /**
   * Reserializes the patched {@link JsonNode} to the base {@link RecordTemplate} definition
   *
   * @param patched the deserialized patched json in custom format per aspect spec
   * @return A {@link JsonNode} that has been retranslated from patch semantics
   */
  @Nonnull
  JsonNode rebaseFields(JsonNode patched);
}
