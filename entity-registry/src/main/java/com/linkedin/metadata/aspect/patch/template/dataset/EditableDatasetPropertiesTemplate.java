package com.linkedin.metadata.aspect.patch.template.dataset;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.metadata.aspect.patch.template.Template;
import javax.annotation.Nonnull;

public class EditableDatasetPropertiesTemplate implements Template<EditableDatasetProperties> {

  @Override
  public EditableDatasetProperties getSubtype(RecordTemplate recordTemplate)
      throws ClassCastException {
    if (recordTemplate instanceof EditableDatasetProperties) {
      return (EditableDatasetProperties) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to EditableDatasetProperties");
  }

  @Override
  public Class<EditableDatasetProperties> getTemplateType() {
    return EditableDatasetProperties.class;
  }

  @Nonnull
  @Override
  public EditableDatasetProperties getDefault() {
    return new EditableDatasetProperties();
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
