package com.linkedin.metadata.aspect.patch.template.glossary;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.aspect.patch.template.Template;
import javax.annotation.Nonnull;

public class GlossaryTermInfoTemplate implements Template<GlossaryTermInfo> {

  @Override
  public GlossaryTermInfo getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof GlossaryTermInfo) {
      return (GlossaryTermInfo) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to GlossaryTermInfo");
  }

  @Override
  public Class<GlossaryTermInfo> getTemplateType() {
    return GlossaryTermInfo.class;
  }

  @Nonnull
  @Override
  public GlossaryTermInfo getDefault() {
    return new GlossaryTermInfo();
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
