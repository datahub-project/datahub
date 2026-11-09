package com.linkedin.metadata.aspect.patch.template.ml;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.aspect.patch.template.Template;
import com.linkedin.ml.metadata.MLModelProperties;
import jakarta.json.JsonPatch;
import javax.annotation.Nonnull;

/**
 * Patch template for the {@link MLModelProperties} aspect.
 *
 * <p>{@code MLModelPropertiesPatchBuilder} appends to {@code trainingMetrics} and {@code
 * hyperParams} with {@code /<field>/-}. A JSON Patch append needs the target array to exist, and
 * both fields are optional, so the template supplies an empty array when the field is absent. If
 * the field was absent before the patch and is still empty after it, it is removed again, so an
 * unrelated patch does not add empty arrays. A stored empty array is kept as it is.
 *
 * <p>Appends are not keyed by name: adding a metric whose name already exists adds a second entry,
 * matching what {@code MLModelPropertiesPatchBuilder} asks for.
 */
public class MLModelPropertiesTemplate implements Template<MLModelProperties> {

  private static final String TRAINING_METRICS_FIELD_NAME = "trainingMetrics";
  private static final String HYPER_PARAMS_FIELD_NAME = "hyperParams";

  @Override
  public MLModelProperties getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof MLModelProperties) {
      return (MLModelProperties) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to MLModelProperties");
  }

  @Override
  public Class<MLModelProperties> getTemplateType() {
    return MLModelProperties.class;
  }

  @Nonnull
  @Override
  public MLModelProperties getDefault() {
    MLModelProperties mlModelProperties = new MLModelProperties();
    mlModelProperties.setCustomProperties(new StringMap());
    return mlModelProperties;
  }

  @Override
  public MLModelProperties applyPatch(RecordTemplate recordTemplate, JsonPatch jsonPatch)
      throws JsonProcessingException {
    MLModelProperties original = getSubtype(recordTemplate);
    boolean hadTrainingMetrics = original.hasTrainingMetrics();
    boolean hadHyperParams = original.hasHyperParams();

    MLModelProperties patched = Template.super.applyPatch(recordTemplate, jsonPatch);

    if (!hadTrainingMetrics
        && patched.hasTrainingMetrics()
        && patched.getTrainingMetrics().isEmpty()) {
      patched.removeTrainingMetrics();
    }
    if (!hadHyperParams && patched.hasHyperParams() && patched.getHyperParams().isEmpty()) {
      patched.removeHyperParams();
    }
    return patched;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    ObjectNode transformed = (ObjectNode) baseNode.deepCopy();
    if (!transformed.has(TRAINING_METRICS_FIELD_NAME)) {
      transformed.set(TRAINING_METRICS_FIELD_NAME, JsonNodeFactory.instance.arrayNode());
    }
    if (!transformed.has(HYPER_PARAMS_FIELD_NAME)) {
      transformed.set(HYPER_PARAMS_FIELD_NAME, JsonNodeFactory.instance.arrayNode());
    }
    return transformed;
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    return patched;
  }
}
