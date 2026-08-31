package com.linkedin.metadata.aspect.patch.template.container;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.container.EditableContainerProperties;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.Template;
import javax.annotation.Nonnull;

public class EditableContainerPropertiesTemplate implements Template<EditableContainerProperties> {

  @Override
  public EditableContainerProperties getSubtype(RecordTemplate recordTemplate)
      throws ClassCastException {
    if (recordTemplate instanceof EditableContainerProperties) {
      return (EditableContainerProperties) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to EditableContainerProperties");
  }

  @Override
  public Class<EditableContainerProperties> getTemplateType() {
    return EditableContainerProperties.class;
  }

  @Nonnull
  @Override
  public EditableContainerProperties getDefault() {
    return new EditableContainerProperties();
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
