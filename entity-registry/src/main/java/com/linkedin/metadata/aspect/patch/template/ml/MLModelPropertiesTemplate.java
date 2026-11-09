package com.linkedin.metadata.aspect.patch.template.ml;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.aspect.patch.template.Template;
import com.linkedin.ml.metadata.MLModelProperties;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Patch template for the {@link MLModelProperties} aspect.
 *
 * <p>{@code MLModelPropertiesPatchBuilder} appends to {@code trainingMetrics} and {@code
 * hyperParams} with {@code /<field>/-}. A JSON Patch append needs the target array to exist, and
 * both fields are optional, so the template supplies an empty array when the field is absent and
 * drops it again if it is still empty after the patch. An aspect that never had metrics therefore
 * does not gain an empty {@code trainingMetrics} array from an unrelated patch.
 */
public class MLModelPropertiesTemplate implements Template<MLModelProperties> {

  static final String TRAINING_METRICS_FIELD_NAME = "trainingMetrics";
  static final String HYPER_PARAMS_FIELD_NAME = "hyperParams";

  private static final List<String> APPENDABLE_ARRAY_FIELDS =
      List.of(TRAINING_METRICS_FIELD_NAME, HYPER_PARAMS_FIELD_NAME);

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

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    ObjectNode transformed = (ObjectNode) baseNode.deepCopy();
    for (String field : APPENDABLE_ARRAY_FIELDS) {
      JsonNode value = transformed.get(field);
      if (value == null || value.isNull()) {
        transformed.set(field, JsonNodeFactory.instance.arrayNode());
      }
    }
    return transformed;
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    ObjectNode rebased = (ObjectNode) patched.deepCopy();
    for (String field : APPENDABLE_ARRAY_FIELDS) {
      JsonNode value = rebased.get(field);
      if (value != null && value.isArray() && value.isEmpty()) {
        rebased.remove(field);
      }
    }
    return rebased;
  }
}
