package com.linkedin.metadata.aspect.patch.template.ml;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.Template;
import com.linkedin.ml.metadata.EditableMLFeatureProperties;
import javax.annotation.Nonnull;

public class EditableMLFeaturePropertiesTemplate implements Template<EditableMLFeatureProperties> {

  @Override
  public EditableMLFeatureProperties getSubtype(RecordTemplate recordTemplate)
      throws ClassCastException {
    if (recordTemplate instanceof EditableMLFeatureProperties) {
      return (EditableMLFeatureProperties) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to EditableMLFeatureProperties");
  }

  @Override
  public Class<EditableMLFeatureProperties> getTemplateType() {
    return EditableMLFeatureProperties.class;
  }

  @Nonnull
  @Override
  public EditableMLFeatureProperties getDefault() {
    return new EditableMLFeatureProperties();
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
}
