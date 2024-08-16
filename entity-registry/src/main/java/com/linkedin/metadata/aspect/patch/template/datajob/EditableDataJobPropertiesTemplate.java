package com.linkedin.metadata.aspect.patch.template.datajob;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datajob.EditableDataJobProperties;
import com.linkedin.metadata.aspect.patch.template.Template;
import javax.annotation.Nonnull;

public class EditableDataJobPropertiesTemplate implements Template<EditableDataJobProperties> {

  @Override
  public EditableDataJobProperties getSubtype(RecordTemplate recordTemplate)
      throws ClassCastException {
    if (recordTemplate instanceof EditableDataJobProperties) {
      return (EditableDataJobProperties) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to EditableDataJobProperties");
  }

  @Override
  public Class<EditableDataJobProperties> getTemplateType() {
    return EditableDataJobProperties.class;
  }

  @Nonnull
  @Override
  public EditableDataJobProperties getDefault() {
    return new EditableDataJobProperties();
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
