package com.linkedin.metadata.aspect.patch.template.application;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.application.ApplicationProperties;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.Template;
import javax.annotation.Nonnull;

public class ApplicationPropertiesTemplate implements Template<ApplicationProperties> {

  @Override
  public ApplicationProperties getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof ApplicationProperties) {
      return (ApplicationProperties) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to ApplicationProperties");
  }

  @Override
  public Class<ApplicationProperties> getTemplateType() {
    return ApplicationProperties.class;
  }

  @Nonnull
  @Override
  public ApplicationProperties getDefault() {
    return new ApplicationProperties();
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
