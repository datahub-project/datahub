package com.linkedin.metadata.aspect.patch.template.ml;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.Template;
import com.linkedin.ml.metadata.EditableMLModelProperties;
import javax.annotation.Nonnull;

public class EditableMLModelPropertiesTemplate implements Template<EditableMLModelProperties> {

  @Override
  public EditableMLModelProperties getSubtype(RecordTemplate recordTemplate)
      throws ClassCastException {
    if (recordTemplate instanceof EditableMLModelProperties) {
      return (EditableMLModelProperties) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to EditableMLModelProperties");
  }

  @Override
  public Class<EditableMLModelProperties> getTemplateType() {
    return EditableMLModelProperties.class;
  }

  @Nonnull
  @Override
  public EditableMLModelProperties getDefault() {
    return new EditableMLModelProperties();
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
