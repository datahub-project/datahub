package com.linkedin.metadata.aspect.patch.template.dataflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datajob.EditableDataFlowProperties;
import com.linkedin.metadata.aspect.patch.template.Template;
import javax.annotation.Nonnull;

public class EditableDataFlowPropertiesTemplate implements Template<EditableDataFlowProperties> {

  @Override
  public EditableDataFlowProperties getSubtype(RecordTemplate recordTemplate)
      throws ClassCastException {
    if (recordTemplate instanceof EditableDataFlowProperties) {
      return (EditableDataFlowProperties) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to EditableDataFlowProperties");
  }

  @Override
  public Class<EditableDataFlowProperties> getTemplateType() {
    return EditableDataFlowProperties.class;
  }

  @Nonnull
  @Override
  public EditableDataFlowProperties getDefault() {
    return new EditableDataFlowProperties();
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
