package com.linkedin.metadata.aspect.patch.template.ml;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.Template;
import com.linkedin.ml.metadata.EditableMLPrimaryKeyProperties;
import javax.annotation.Nonnull;

public class EditableMLPrimaryKeyPropertiesTemplate
    implements Template<EditableMLPrimaryKeyProperties> {

  @Override
  public EditableMLPrimaryKeyProperties getSubtype(RecordTemplate recordTemplate)
      throws ClassCastException {
    if (recordTemplate instanceof EditableMLPrimaryKeyProperties) {
      return (EditableMLPrimaryKeyProperties) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to EditableMLPrimaryKeyProperties");
  }

  @Override
  public Class<EditableMLPrimaryKeyProperties> getTemplateType() {
    return EditableMLPrimaryKeyProperties.class;
  }

  @Nonnull
  @Override
  public EditableMLPrimaryKeyProperties getDefault() {
    return new EditableMLPrimaryKeyProperties();
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
