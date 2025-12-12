package com.linkedin.metadata.aspect.patch.template.ml;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.Template;
import com.linkedin.ml.metadata.EditableMLModelGroupProperties;
import javax.annotation.Nonnull;

public class EditableMLModelGroupPropertiesTemplate
    implements Template<EditableMLModelGroupProperties> {

  @Override
  public EditableMLModelGroupProperties getSubtype(RecordTemplate recordTemplate)
      throws ClassCastException {
    if (recordTemplate instanceof EditableMLModelGroupProperties) {
      return (EditableMLModelGroupProperties) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to EditableMLModelGroupProperties");
  }

  @Override
  public Class<EditableMLModelGroupProperties> getTemplateType() {
    return EditableMLModelGroupProperties.class;
  }

  @Nonnull
  @Override
  public EditableMLModelGroupProperties getDefault() {
    return new EditableMLModelGroupProperties();
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
