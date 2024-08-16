package com.linkedin.metadata.aspect.patch.template.dashboard;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.dashboard.EditableDashboardProperties;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.Template;
import javax.annotation.Nonnull;

public class EditableDashboardPropertiesTemplate implements Template<EditableDashboardProperties> {

  @Override
  public EditableDashboardProperties getSubtype(RecordTemplate recordTemplate)
      throws ClassCastException {
    if (recordTemplate instanceof EditableDashboardProperties) {
      return (EditableDashboardProperties) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to EditableDashboardProperties");
  }

  @Override
  public Class<EditableDashboardProperties> getTemplateType() {
    return EditableDashboardProperties.class;
  }

  @Nonnull
  @Override
  public EditableDashboardProperties getDefault() {
    return new EditableDashboardProperties();
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
