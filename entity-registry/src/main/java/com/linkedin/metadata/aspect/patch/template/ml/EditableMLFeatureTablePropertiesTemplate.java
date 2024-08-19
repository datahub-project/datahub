package com.linkedin.metadata.aspect.patch.template.ml;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.Template;
import com.linkedin.ml.metadata.EditableMLFeatureTableProperties;
import javax.annotation.Nonnull;

public class EditableMLFeatureTablePropertiesTemplate
    implements Template<EditableMLFeatureTableProperties> {

  @Override
  public EditableMLFeatureTableProperties getSubtype(RecordTemplate recordTemplate)
      throws ClassCastException {
    if (recordTemplate instanceof EditableMLFeatureTableProperties) {
      return (EditableMLFeatureTableProperties) recordTemplate;
    }
    throw new ClassCastException(
        "Unable to cast RecordTemplate to EditableMLFeatureTableProperties");
  }

  @Override
  public Class<EditableMLFeatureTableProperties> getTemplateType() {
    return EditableMLFeatureTableProperties.class;
  }

  @Nonnull
  @Override
  public EditableMLFeatureTableProperties getDefault() {
    return new EditableMLFeatureTableProperties();
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
