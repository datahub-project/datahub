package com.linkedin.metadata.aspect.patch.template.chart;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.chart.EditableChartProperties;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.Template;
import javax.annotation.Nonnull;

public class EditableChartPropertiesTemplate implements Template<EditableChartProperties> {

  @Override
  public EditableChartProperties getSubtype(RecordTemplate recordTemplate)
      throws ClassCastException {
    if (recordTemplate instanceof EditableChartProperties) {
      return (EditableChartProperties) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to EditableChartProperties");
  }

  @Override
  public Class<EditableChartProperties> getTemplateType() {
    return EditableChartProperties.class;
  }

  @Nonnull
  @Override
  public EditableChartProperties getDefault() {
    return new EditableChartProperties();
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
