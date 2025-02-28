package com.linkedin.metadata.aspect.patch.template.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.domain.DomainProperties;
import com.linkedin.metadata.aspect.patch.template.Template;
import javax.annotation.Nonnull;

public class DomainPropertiesTemplate implements Template<DomainProperties> {

  @Override
  public DomainProperties getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof DomainProperties) {
      return (DomainProperties) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to DomainProperties");
  }

  @Override
  public Class<DomainProperties> getTemplateType() {
    return DomainProperties.class;
  }

  @Nonnull
  @Override
  public DomainProperties getDefault() {
    return new DomainProperties();
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
